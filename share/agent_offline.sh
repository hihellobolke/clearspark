#!/bin/bash
#
# Copyright 2014 Gautam R. Singh <g@ut.am>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


clear
{
echo "
Help:
-----
 $0 N S FILE

       N -------   Runs collector (lsof,ps) N times,
                   i.e. number of iterations to run,
                   [Default is 10]

       S -------   Seconds to sleep between each run
                   [Default is 60 Seconds]

       FILE ----   Writes to the given file, if no
                   file is specified, outputs to
                   stdout.

"
echo "Info:"
echo "------"
echo '   This script dumps output of few commands to a plain text file'
echo '   the dump file must be read by -c-s- agent, to parse dependencies'
echo '   commands runby script are ip, lsof, cat /proc/mounts, host, ps, date etc '
echo '   To generate full dependencies of host, run it as root'
echo
} 1>&2

N=${1:-10}
S=${2:-60}
L=${3:-/dev/stdout}
{

echo " In this execution:"
echo "-------------------"
echo "   Script will collect output $N times, at interval of $S seconds"
echo "   The output will be writtend to $L"
echo
} 1>&2

[[ $# == 0 ]] && read -p "Press [Enter] key to continue..."

set -e

{
    type=host
    marker="### $type ###"
    echo $marker
    hostname
    echo $marker
} >> $L


{
    type=ip
    marker="### $type ###"
    echo $marker
    ip addr show
    echo $marker
} >> $L

{
    type=mounts
    marker="### $type ###"
    echo $marker
    awk '!/fuse\./{cmd="stat "$2" -c 0x%D";
                   cmd | getline ss;
                   close (cmd);
                   printf "%s %s %s %s\n", $1, $2, ss, $3}'  /proc/mounts
    echo $marker
} >> $L

{
    type=time
    marker="### $type ###"
    echo $marker
    date +%s
    echo $marker
} >> $L


for (( c=1; c<=$N; c++ )); do
    {
        type=lsof
        marker="### $type ###"
        echo $marker
        echo '### ps ###'
        ps -e -o pid,user,args
        echo '### ps ###'
        lsof -FkugpRDtnc0 -l -nP -w
        date +mmmmm%smmmmm
        echo $marker
    }
    sleep $S
done >> $L

