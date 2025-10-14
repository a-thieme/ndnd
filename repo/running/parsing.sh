#!/bin/bash
find "running/logs" -type f -exec grep -Hn 'jobs="/' {} +
find "running/logs" -type f -exec grep -Hn 'jobs=""' {} +

