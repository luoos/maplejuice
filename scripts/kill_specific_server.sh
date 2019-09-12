server="junluo2@fa19-cs425-g17-$1.cs.illinois.edu"
ssh $server "kill \$(ps -aux|grep server|grep -v grep|awk '{print \$2}');"
