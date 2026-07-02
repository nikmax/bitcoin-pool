#!/bin/bash

cd /btc
pip install --no-cache-dir -r requirements.txt
cd pool
python3 pool.py
