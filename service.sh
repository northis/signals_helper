#!/bin/bash

sudo cp channel-forwarder.service /lib/systemd/system
sudo systemctl daemon-reload
sudo systemctl start channel-forwarder.service
sudo systemctl status channel-forwarder.service

# sudo systemctl stop channel-forwarder.service
