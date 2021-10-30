#!/bin/bash

pip-compile requirements.in --allow-unsafe
pip-sync
