mkdir scripts/logs
python ./scripts/generate_random_lines.py
for i in {1..9}; do scp scripts/logs/random$i\.log fa19-cs425-g17-0$i\.cs.illinois.edu:; done
scp scripts/logs/random10.log fa19-cs425-g17-10.cs.illinois.edu:
rm -rf scripts/logs
