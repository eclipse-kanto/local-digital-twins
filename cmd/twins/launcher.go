// Copyright (c) 2022 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// https://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0

package main

import (
	"context"
	"os"
	"syscall"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/pkg/errors"

	"github.com/eclipse-kanto/suite-connector/cache"
	"github.com/eclipse-kanto/suite-connector/cmd/connector/app"
	"github.com/eclipse-kanto/suite-connector/config"
	"github.com/eclipse-kanto/suite-connector/logger"
	"github.com/eclipse-kanto/suite-connector/routing"

	conn "github.com/eclipse-kanto/suite-connector/connector"

	"github.com/eclipse-kanto/local-digital-twins/internal/commands"
	"github.com/eclipse-kanto/local-digital-twins/internal/persistence"
	"github.com/eclipse-kanto/local-digital-twins/internal/sync"
)

// Launcher contains the launch system data.
type launcher struct {
	manager   conn.SubscriptionManager
	statusPub message.Publisher

	localClient *conn.MQTTConnection

	done    chan bool
	signals chan os.Signal
}

func newLauncher(client *conn.MQTTConnection, pub message.Publisher, manager conn.SubscriptionManager) app.Launcher {
	return &launcher{
		statusPub:   pub,
		manager:     manager,
		localClient: client,
		done:        make(chan bool, 2),
		signals:     make(chan os.Signal, 2),
	}
}

func (l *launcher) Run(
	cleanSession bool,
	global config.SettingsAccessor,
	args map[string]interface{},
	logger logger.Logger,
) error {
	settings := global.DeepCopy().(*TwinSettings)

	if err := config.ApplySettings(cleanSession, settings, args, l.statusPub, logger); err != nil {
		return err
	}

	router := app.NewRouter(logger)

	cloudClient, err := config.CreateCloudConnection(settings.LocalConnection(), false, logger)
	if err != nil {
		return errors.Wrap(err, "cannot create mosquitto connection")
	}

	deviceInfo := commands.DeviceInfo{
		DeviceID:         settings.Settings.DeviceID,
		TenantID:         settings.Settings.TenantID,
		AutoProvisioning: settings.Settings.AutoProvisioningEnabled,
	}
	logger.Infof("Launching with device info %+v", deviceInfo)

	honoClient, cleanup, err := config.CreateHubConnection(settings.HubConnection(), true, logger)
	if err != nil {
		routing.SendStatus(routing.StatusConnectionError, l.statusPub, logger)
		return errors.Wrap(err, "cannot create Hub connection")
	}
	l.manager.ForwardTo(honoClient)

	reqCache := cache.NewTTLCache()

	honoPub := config.NewOnlineHonoPub(logger, honoClient)
	honoSub := config.NewHonoSub(logger, honoClient)

	mosquittoSub := conn.NewSubscriber(cloudClient, conn.QosAtLeastOnce, false, logger, nil)
	mosquittoPub := conn.NewPublisher(cloudClient, conn.QosAtLeastOnce, logger, nil)

	routing.CommandsResBus(router, honoPub, mosquittoSub, reqCache)

	storage, err := persistence.NewThingsDB(settings.ThingsDb, settings.DeviceID)
	if err != nil {
		return errors.Wrap(err, "failed to create Things DB")
	}
	logger.Info("Things DB is opened", watermill.LogFields{
		"path":     settings.ThingsDb,
		"deviceID": storage.GetDeviceID(),
	})

	routing.TelemetryBus(router, honoPub, mosquittoSub)

	eventsBus(router, honoPub, cloudClient, deviceInfo, storage, logger)

	handler := routing.CommandsReqBus(router, mosquittoPub, honoSub, reqCache)

	synchronizer := &sync.Synchronizer{
		DeviceInfo:   deviceInfo,
		HonoPub:      honoPub,
		MosquittoPub: mosquittoPub,
		Storage:      storage,
		Logger:       logger,
	}
	handler.AddMiddleware(syncMiddleware(logger, synchronizer))

	paramsPub := conn.NewPublisher(cloudClient, conn.QosAtMostOnce, logger, nil)
	paramsSub := conn.NewSubscriber(cloudClient, conn.QosAtMostOnce, true, logger, nil)

	params := routing.NewGwParams(settings.DeviceID, settings.TenantID, settings.PolicyID)
	routing.ParamsBus(router, params, paramsPub, paramsSub, logger)

	shutdown := func(r *message.Router) error {
		go func() {
			defer func() {
				routing.SendStatus(routing.StatusConnectionClosed, l.statusPub, logger)

				reqCache.Close()

				cleanup()

				storage.Close()

				logger.Info("Messages router stopped", nil)
				l.done <- true
			}()

			<-r.Running()

			statusHandler := &routing.ConnectionStatusHandler{
				Pub:    l.statusPub,
				Logger: logger,
			}
			cloudClient.AddConnectionListener(statusHandler)
			defer cloudClient.RemoveConnectionListener(statusHandler)

			errorsHandler := &routing.ErrorsHandler{
				StatusPub: l.statusPub,
				Logger:    logger,
			}
			honoClient.AddConnectionListener(errorsHandler)
			defer honoClient.RemoveConnectionListener(errorsHandler)

			synchronizeHandler := &synchronizeHandler{
				synchronizer: synchronizer,
				logger:       logger,
			}
			honoClient.AddConnectionListener(synchronizeHandler)
			defer honoClient.RemoveConnectionListener(synchronizeHandler)

			if err := config.LocalConnect(context.Background(), cloudClient, logger); err != nil {
				logger.Error("Cannot connect to local broker", err, nil)
				app.StopRouter(r)
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), hubParamsAnnounceTimeout())
			defer cancel()

			l.pushGwParams(ctx, params, logger)

			err := config.HonoConnect(l.signals, l.statusPub, honoClient, logger)

			if !errors.Is(err, context.Canceled) {
				defer honoClient.Disconnect()

				<-l.signals
			}

			honoClient.RemoveConnectionListener(synchronizeHandler)
			honoClient.RemoveConnectionListener(errorsHandler)
			cloudClient.RemoveConnectionListener(statusHandler)

			cloudClient.Disconnect()

			app.StopRouter(r)
		}()

		return nil
	}
	router.AddPlugin(shutdown)

	app.StartRouter(router)

	return nil
}

func (l *launcher) Stop() {
	if l == nil {
		return
	}

	l.signals <- syscall.SIGTERM

	<-l.done
}

func (l *launcher) pushGwParams(ctx context.Context, params *routing.GwParams, logger logger.Logger) {
	go func() {
		<-ctx.Done()

		if !errors.Is(ctx.Err(), context.Canceled) {
			routing.SendGwParams(params, false, l.statusPub, logger)
		}
	}()
}
