python ./scripts/generate_random_lines.py
for i in {1..10}; do scp scripts/logs/random$i\.log vm$i\_425:/usr/logs/; done
