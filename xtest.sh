#! /bin/bash

cmd1="./rowdump1 -s"
cmd2="./rowdump2 -s"
cmd3="./rowdump1 -z"
cmd4="./rowdump1 -x"
#cmd5="./rowdump1 -f"
#cmd6="./rowdump2 -f"

time="/usr/bin/time -f %U -o time.tmp"

query1="select 10000,200,300000,rpad('x',30,'z') from generate_series(1,5000000)"
query2="select rpad('x',10,'z'),rpad('x',20,'z'),rpad('x',30,'z'),rpad('x',40,'z'),rpad('x',50,'z'),rpad('x',60,'z') from generate_series(1,3000000)"
query3="select `seq -s, 1 100` from generate_series(1,800000)"
query4="select 1000,rpad('x', 400, 'z'),rpad('x', 4000, 'z') from generate_series(1,100000)"

set -e

for qn in 1 2 3 4; do
  ref=""
  qvar=query${qn}
  query="${!qvar}"
  printf "QUERY: ${query}\n"
  for n in 1 2 3 4; do
    cvar=cmd${n}
    cmd="${!cvar}"
    printf "${cmd}:  "
    ${time} ${cmd} -c "${query}" > /dev/null
    t1=`cat time.tmp`
    printf "%5s  " "$t1"
    ${time} ${cmd} -c "${query}" > /dev/null
    t2=`cat time.tmp`
    printf "%5s  " "$t2"
    ${time} ${cmd} -c "${query}" > /dev/null
    t3=`cat time.tmp`
    printf "%5s  " "$t3"
    avg=`awk "BEGIN { print ($t1 + $t2 + $t3) / 3 } "`
    test -n "$ref" || ref="$avg"
    per=`awk "BEGIN { print 100.0 * $avg / $ref } "`
    printf "avg: %5.2f [ %5.2f %% ]\n" "$avg" "$per"
  done
done

