#!/bin/bash

# GreenHadoop makes Hadoop aware of solar energy availability.
# http://www.research.rutgers.edu/~goiri/
# Copyright (C) 2012 Inigo Goiri, Rutgers University
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>

export HADOOP_HOME="/home/goiri/hadoop-0.21.0"

MASTER_NODE="crypt15"

function experiment {
	NAME=$1
	shift
	FLAGS=$*

	echo "name="$NAME" flags="$FLAGS

	touch $HADOOP_HOME/logs/hadoop-goiri-jobtracker-$MASTER_NODE.log
	touch $HADOOP_HOME/logs/hadoop-goiri-namenode-$MASTER_NODE.log

	# Clean system
	echo "Cleanning..."
	cleaned=0
	retries=3
	while (( "$cleaned" == "0" )) && (( "$retries" > "0" )); do
		cleaned=1
		./ghadoopclean.py > /dev/null &
		TESTPID=$!
		a=0
		while ps -p $TESTPID >/dev/null; do
			sleep 1
			let a=a+1
			if [ $a -gt 400 ]; then
				echo "Clean failed: try again..."
				kill -9 $TESTPID
				cleaned=0
				let retries=retries-1
			fi
		done
	done

	# Date
	echo "Setting time..."
	AUXDATE=`date`
	ssh root@crypt15 $HADOOP_HOME/bin/slaves.sh date -s \"$AUXDATE\" >/dev/null

	sleep 1
	echo "Starting..."

	rm -f logs/ghadoop-jobs.log
	rm -f logs/ghadoop-energy.log
	rm -f logs/ghadoop-scheduler.log
	rm -f logs/ghadoop-error.log
	touch logs/ghadoop-error.log
	sleep 1
	./ghadoopd $FLAGS

	# Save results
	mkdir -p logs/$NAME
# 	echo $NAME > logs/$NAME/$NAME-summary.log
	./ghadoopparser.py logs/ghadoop >> logs/$NAME/$NAME-summary.html

	mv logs/ghadoop-jobs.log logs/$NAME/$NAME-jobs.log
	mv logs/ghadoop-energy.log logs/$NAME/$NAME-energy.log
	mv logs/ghadoop-scheduler.log logs/$NAME/$NAME-scheduler.log
	mv logs/ghadoop-error.log logs/$NAME/$NAME-error.log

	# Plot results: energy
# 	echo "set term svg size 1280,600" > logs/$NAME/$NAME.plot
	echo "set term svg size 960,450" > logs/$NAME/$NAME.plot
	echo "set out \"logs/$NAME/$NAME-energy.svg\"" >> logs/$NAME/$NAME.plot
	echo "set ylabel \"Power (kW)\"" >> logs/$NAME/$NAME.plot
# 	echo "set yrange [0:1.8]" >> logs/$NAME/$NAME.plot
	echo "set yrange [0:2.4]" >> logs/$NAME/$NAME.plot

	echo "set y2label \"Brown energy price ($/kWh)\"" >> logs/$NAME/$NAME.plot
	echo "set y2range [0:0.3]" >> logs/$NAME/$NAME.plot
	echo "set y2tics" >> logs/$NAME/$NAME.plot
	echo "set ytics nomirror" >> logs/$NAME/$NAME.plot

	echo "set xdata time" >> logs/$NAME/$NAME.plot
	echo "set timefmt \"%s\"" >> logs/$NAME/$NAME.plot
	echo "set format x \"%a\n%R\"" >> logs/$NAME/$NAME.plot
	echo "set format x \"%a %R\"" >> logs/$NAME/$NAME.plot

	echo "set style fill solid" >> logs/$NAME/$NAME.plot
	echo "plot \"logs/$NAME/$NAME-energy.log\" using (\$1*24):(\$10/1000) lc rgb \"#808080\" w filledcurve title \"Brown consumed\",\\" >> logs/$NAME/$NAME.plot
	echo "\"logs/$NAME/$NAME-energy.log\" using (\$1*24):(\$8/1000) lc rgb \"#e6e6e6\" w filledcurve title \"Green consumed\",\\" >> logs/$NAME/$NAME.plot
# 	echo "\"logs/$NAME/$NAME-energy.log\" using (\$1+(2*24*24+10*24)):(\$2/1000) lw 2 lc rgb \"black\" w steps title \"Green predicted\",\\" >> logs/$NAME/$NAME.plot
	echo "\"logs/$NAME/$NAME-energy.log\" using (\$1*24):(\$3/1000) lw 2 lc rgb \"black\" w steps title \"Green predicted\",\\" >> logs/$NAME/$NAME.plot
	echo "\"logs/$NAME/$NAME-energy.log\" using (\$1*24):4 axes x1y2 lw 2 lc rgb \"black\" w steps title \"Brown price\"" >> logs/$NAME/$NAME.plot

	gnuplot logs/$NAME/$NAME.plot

	# Plot results: nodes
# 	echo "set term svg size 1280,600" > logs/$NAME/$NAME.plot
	echo "set term svg size 960,450" > logs/$NAME/$NAME-nodes.plot
	echo "set out \"logs/$NAME/$NAME-nodes.svg\"" >> logs/$NAME/$NAME-nodes.plot
	echo "set ylabel \"Nodes\"" >> logs/$NAME/$NAME-nodes.plot
	echo "set yrange [0:16]" >> logs/$NAME/$NAME-nodes.plot

	echo "set xdata time" >> logs/$NAME/$NAME-nodes.plot
	echo "set timefmt \"%s\"" >> logs/$NAME/$NAME-nodes.plot
	echo "set format x \"%a\n%R\"" >> logs/$NAME/$NAME-nodes.plot
	echo "set format x \"%a %R\"" >> logs/$NAME/$NAME-nodes.plot

	echo "set style fill solid" >> logs/$NAME/$NAME-nodes.plot
	echo "plot \"logs/$NAME/$NAME-energy.log\" using (\$1*24):7 lw 2 lc rgb \"#C0C0C0\" w filledcurve title \"Dec nodes\",\\" >> logs/$NAME/$NAME-nodes.plot
	echo "\"logs/$NAME/$NAME-energy.log\" using (\$1*24):6 lc rgb \"#909090\" w filledcurve title \"Up nodes\",\\" >> logs/$NAME/$NAME-nodes.plot
	echo "\"logs/$NAME/$NAME-energy.log\" using (\$1*24):5 lc rgb \"#404040\" w filledcurve title \"Run nodes\"" >> logs/$NAME/$NAME-nodes.plot

	gnuplot logs/$NAME/$NAME-nodes.plot
}



# DATE_MOST="2010-5-31T09:00:00"
# SOLAR_MOST="data/solarpower-31-05-2010" # Best energy
BROWN_SUMMER="data/browncost-onoffpeak-summer.nj"
BROWN_WINTER="data/browncost-onoffpeak-winter.nj"

PEAK_WINTER=5.5884 # Winter October-May
PEAK_SUMMER=13.6136 # Summer June-Sep

# High High
DATE_1="2011-5-9T00:00:00"
SOLAR_1="data/solarpower-09-05-2011"
BROWN_1=$BROWN_WINTER
PEAK_1=$PEAK_WINTER

# High Medium
DATE_2="2011-5-12T00:00:00"
SOLAR_2="data/solarpower-12-05-2011"
BROWN_2=$BROWN_WINTER
PEAK_2=$PEAK_WINTER

# Medium High
DATE_3="2011-6-14T00:00:00"
SOLAR_3="data/solarpower-14-06-2011"
BROWN_3=$BROWN_SUMMER
PEAK_3=$PEAK_SUMMER

# Medium Medium
DATE_4="2011-6-16T00:00:00"
SOLAR_4="data/solarpower-16-06-2011"
BROWN_4=$BROWN_SUMMER
PEAK_4=$PEAK_SUMMER

# Low Low
DATE_0="2011-5-15T00:00:00"
SOLAR_0="data/solarpower-15-05-2011"
BROWN_0=$BROWN_WINTER
PEAK_0=$PEAK_WINTER



# Workloads
WORKLOAD_LOADGEN_030="workload/workload-continuous-030-loadgen"

WORKLOAD_HETE="workload/workload-hete"
WORKLOAD_BIGCHANGE="workload/workload-bigchange"

WORKLOAD_RED_015="workload/workload-continuous-015-red"
WORKLOAD_RED_030="workload/workload-continuous-030-red"
WORKLOAD_RED_060="workload/workload-continuous-060-red"
WORKLOAD_RED_090="workload/workload-continuous-090-red"

# Not used
# WORKLOAD_CONT_015="workload/workload-continuous-015"
# WORKLOAD_CONT_030="workload/workload-continuous-030"
# WORKLOAD_CONT_060="workload/workload-continuous-060"
# WORKLOAD_CONT_090="workload/workload-continuous-090"
# 
# WORKLOAD_CONT_045="workload/workload-continuous-045"
# WORKLOAD_CONT_075="workload/workload-continuous-075"
# WORKLOAD_CONT_100="workload/workload-continuous-100"

# Nutch
# WORKLOAD_NUTCH="workload/workload-nutch-030"
WORKLOAD_NUTCH2="workload/workload-nutch-008"

WORKLOAD_HIGH1="workload/workload-continuous-high1-3"
WORKLOAD_HIGH2="workload/workload-continuous-high2-3"

TASK_ENERGY_015=0.30
TASK_ENERGY_030=0.35
TASK_ENERGY_060=0.45
TASK_ENERGY_090=0.55

TASK_ENERGY_NUTCH=0.14

REPLICATION=10

# After submission
# experiment "bigchange2-greenpeak-day1-30seconds"              --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_BIGCHANGE
experiment "bigchange3-hadooppm-day1-30seconds"              --energy --nobrown --nogreen --peak 0.0 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_BIGCHANGE

# Experiments start
# Asumption and utilization
# experiment "10-greenpeak-day1-15seconds"              --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_015 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_RED_015
# experiment "11-greenpeak-day1-30seconds"              --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_RED_030
# experiment "12-greenpeak-day1-60seconds"              --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_060 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_RED_060
# experiment "13-greenpeak-day1-90seconds"              --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_090 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_RED_090

# experiment "20-hadoop-day1-30seconds"                 --nobrown --nogreen --peak 0.0 --repl 0 --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_RED_030
# experiment "21-hadooppm-day1-30seconds"               --energy --nobrown --nogreen --peak 0.0 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_RED_030

# GreenOnly
# experiment "22-greenonly-day1-30seconds"              --energy --nobrown --peak 0.0 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_RED_030

# GreenVarPrices
# experiment "30-greenvarprices-day1-30seconds"         --energy --peak 0.0 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_RED_030

# Perfect vs unknown
# experiment "50-greenpeak-day1-30seconds-perfect"      --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_RED_030
# experiment "51-greenpeak-day2-30seconds-perfect"      --energy --peak $PEAK_2 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_2 --speedup 24 --brownfile $BROWN_2 --greenfile $SOLAR_2 -w $WORKLOAD_RED_030
# experiment "52-greenpeak-day3-30seconds-perfect"      --energy --peak $PEAK_3 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_3 --speedup 24 --brownfile $BROWN_3 --greenfile $SOLAR_3 -w $WORKLOAD_RED_030
# experiment "53-greenpeak-day4-30seconds-perfect"      --energy --peak $PEAK_4 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_4 --speedup 24 --brownfile $BROWN_4 --greenfile $SOLAR_4 -w $WORKLOAD_RED_030
# experiment "54-greenpeak-day0-30seconds-perfect"      --energy --peak $PEAK_0 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_0 --speedup 24 --brownfile $BROWN_0 --greenfile $SOLAR_0 -w $WORKLOAD_RED_030

# Different days
# experiment "55-greenpeak-day2-30seconds"              --energy --peak $PEAK_2 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_2 --speedup 24 --brownfile $BROWN_2 --greenfile $SOLAR_2 --pred -w $WORKLOAD_RED_030
# experiment "56-greenpeak-day3-30seconds"              --energy --peak $PEAK_3 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_3 --speedup 24 --brownfile $BROWN_3 --greenfile $SOLAR_3 --pred -w $WORKLOAD_RED_030
# experiment "57-greenpeak-day4-30seconds"              --energy --peak $PEAK_4 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_4 --speedup 24 --brownfile $BROWN_4 --greenfile $SOLAR_4 --pred -w $WORKLOAD_RED_030
# experiment "58-greenpeak-day0-30seconds"              --energy --peak $PEAK_0 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_0 --speedup 24 --brownfile $BROWN_0 --greenfile $SOLAR_0 --pred -w $WORKLOAD_RED_030

# experiment "63-hadooppm-day1-15seconds"               --energy --nobrown --nogreen --peak 0.0 --repl $REPLICATION --taskenergy $TASK_ENERGY_015 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_RED_015
# experiment "64new2-hadooppm-day1-60seconds"               --energy --nobrown --nogreen --peak 0.0 --repl $REPLICATION --taskenergy 100.0 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_RED_060
# experiment "99-hadooppm-day1-hete"                     --energy --nobrown --nogreen --peak 0.0 --repl $REPLICATION --taskenergy 100.0 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_HETE
# experiment "65new2-hadooppm-day1-90seconds"               --energy --nobrown --nogreen --peak 0.0 --repl $REPLICATION --taskenergy 100.0 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_RED_090
#experiment "21new-hadooppm-day1-30seconds"               --energy --nobrown --nogreen --peak 0.0 --repl $REPLICATION --taskenergy 100.0 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_RED_030
# experiment "57-2-greenpeak-day4-30seconds"              --energy --peak $PEAK_4 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_4 --speedup 24 --brownfile $BROWN_4 --greenfile $SOLAR_4 --pred -w $WORKLOAD_RED_030

# experiment "66-hadoop-day1-15seconds"                 --nobrown --nogreen --peak 0.0 --repl 0 --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_RED_015
# experiment "67-hadoop-day1-60seconds"                 --nobrown --nogreen --peak 0.0 --repl 0 --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_RED_060
# experiment "68-hadoop-day1-90seconds"                 --nobrown --nogreen --peak 0.0 --repl 0 --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_RED_090

# Deadlines
# experiment "91-greenpeak-deadline900-day1-30seconds"  --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_RED_030 --deadline 900
# experiment "92-greenpeak-deadline1800-day1-30seconds" --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_RED_030 --deadline 1800

# Workloads
# experiment "80-hadoop-day1-30seconds-high1"           --nobrown --nogreen --peak 0.0 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_HIGH1
# experiment "81-hadooppm-day1-30seconds-high1"         --energy --nobrown --nogreen --peak 0.0 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_HIGH1
#experiment "82-greenpeak-day1-30seconds-high1"        --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_HIGH1

# experiment "83-hadoop-day1-30seconds-high2"           --nobrown --nogreen --peak 0.0 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_HIGH2
#experiment "84-hadooppm-day1-30seconds-high2"         --energy --nobrown --nogreen --peak 0.0  --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_HIGH2
#experiment "85-greenpeak-day1-30seconds-high2"        --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_030 -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_HIGH2


# Nutch
#experiment "77-hadooppm-day1-nutch"                 --energy --nobrown --nogreen --peak 0.0 --repl $REPLICATION --taskenergy $TASK_ENERGY_NUTCH -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_NUTCH2
#experiment "76-greenpeak-day1-nutch"                --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_NUTCH -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_NUTCH2
#experiment "78-hadoop-day1-nutch"                   --nobrown --nogreen --peak 0.0 --repl 0 --taskenergy $TASK_ENERGY_NUTCH -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_NUTCH2

# experiment "97-hadoop-deadline900-day1-nutch2"        --nobrown --nogreen --peak 0.0 --repl 0 --taskenergy $TASK_ENERGY_NUTCH -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 -w $WORKLOAD_NUTCH2 --deadline 900
#experiment "96-greenpeak-deadline900-day1-nutch"      --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_NUTCH -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_NUTCH2 --deadline 900
#experiment "97-greenpeak-deadline1800-day1-nutch"     --energy --peak $PEAK_1 --repl $REPLICATION --taskenergy $TASK_ENERGY_NUTCH -d $DATE_1 --speedup 24 --brownfile $BROWN_1 --greenfile $SOLAR_1 --pred -w $WORKLOAD_NUTCH2 --deadline 1800

# Heterogeneous

