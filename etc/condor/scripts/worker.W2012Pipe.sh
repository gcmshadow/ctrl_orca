#!/bin/sh

cd /scratch/00342/ux453102/datarel-runs/w2012prod_im0139

export HOME=/home1/00342/ux453102
export HERE=$PWD

export SHELL=/bin/sh
export USER=ux453102
source /etc/bashrc

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/home1/00342/ux453102/libXss/install/lib

# module avail
# module swap pgi gcc/4.4.5

module add gcc/4.4.5

echo ${HERE}

type -a gcc

date 

/bin/hostname -f 
shost=`/bin/hostname`

# $0 is the name of the command
# $1 first parameter
# $2 second parameter
# $3 third parameter etc. etc
# $# total number of parameters
# $@ all the parameters will be listed


echo hello >> logs/debuglog
echo $1 >> logs/debuglog
echo hello2 >> logs/debuglog
echo $2 >> logs/debuglog
echo hello3 >> logs/debuglog
echo $3 >> logs/debuglog

visit_raft_sensor=$1" "$2" "$3 

echo full >> logs/debuglog
echo $visit_raft_sensor >> logs/debuglog


echo _CONDOR_SLOT
echo ${_CONDOR_SLOT}
cslot=${_CONDOR_SLOT}

delay=$(( 4 * $cslot )) 
echo delay
echo $delay
echo af_delay

# runid=r100015
#  base=/scratch/00342/ux453102/setup/runs
# rundir=${base}/${runid}/${_CONDOR_SLOT}
# cd ${rundir}
# touch ${_CONDOR_SLOT}

rundir=${HERE}

# sleep ${delay}
logdir=${rundir}/setup-logs
runlogdir=${rundir}/logs
htmldir=${rundir}/html

mkdir -p ${logdir}
mkdir -p ${runlogdir}
mkdir -p ${htmldir}

# export LSST_HOME=/share1/projects/xsede/lsst/beta-0215/lsst_home
export LSST_HOME=/work/00342/ux453102/beta-0215/lsst_home

cd ${LSST_HOME}
pwd 
source loadLSST.sh > ${logdir}/sourceLoadLSST-${shost}-slot${cslot}.log 2>&1

nlines=`wc -l ${logdir}/sourceLoadLSST-${shost}-slot${cslot}.log`
arr1=( `echo "$nlines" | tr -s ' ' ' '` )
num=${arr1[0]}

c1=1
while [ $num -gt 0 ]
do
        echo "Not zero: redo sourceLoadLSST "
        c1++
        sleep 30
        sleep ${delay}
        source loadLSST.sh  > ${logdir}/sourceLoadLSST-${shost}-${c1}-slot${cslot}.log 2>&1
        nlines=`wc -l ${logdir}/sourceLoadLSST-${shost}-${c1}-slot${cslot}.log`
        arr1=( `echo "$nlines" | tr -s ' ' ' '` )
        num=${arr1[0]}
done

setup  -t Winter2012  datarel > ${logdir}/setupDatarel-${shost}-slot${cslot}.log 2>&1

nlines2=`wc -l ${logdir}/setupDatarel-${shost}-slot${cslot}.log`
arr2=( `echo "$nlines2" | tr -s ' ' ' '` )
num2=${arr2[0]}

c2=1
while [ $num2 -gt 0 ]
do 
        echo "Not zero: redo Datarel"
        c2++
        sleep 30
        sleep ${delay}
        setup  -t Winter2012  datarel > ${logdir}/setupDatarel-${shost}-${c2}-slot${cslot}.log 2>&1
        nlines2=`wc -l ${logdir}/setupDatarel-${shost}-${c2}-slot${cslot}.log`
        arr2=( `echo "$nlines2" | tr -s ' ' ' '` )
        num2=${arr2[0]}
done

echo "setting astrometry_net_data"
setup astrometry_net_data > ${logdir}/setupAstrom-${shost}-slot${cslot}.log 2>&1

nlines3=`wc -l ${logdir}/setupAstrom-${shost}-slot${cslot}.log`
arr3=( `echo "$nlines3" | tr -s ' ' ' '` )
num3=${arr3[0]}

c3=1
while [ $num3 -gt 0 ]
do
        echo "Not zero: redo Astrom"
        c3++
        sleep 30
        sleep ${delay}
        setup astrometry_net_data > ${logdir}/setupAstrom-${shost}-${c3}-slot${cslot}.log 2>&1

        nlines3=`wc -l ${logdir}/setupAstrom-${shost}-${c3}-slot${cslot}.log`
        arr3=( `echo "$nlines3" | tr -s ' ' ' '` )
        num3=${arr3[0]}
done

echo "set up astrometry_net_data"

echo "setting up datarel 1928"
cd /share1/projects/xsede/lsst/beta-0215/add-ons/git-datarel-1928-c/LSST/DMS/datarel
setup -r . > ${logdir}/setupDatarel-1928-${shost}-slot${cslot}.log 2>&1

nlines4=`wc -l ${logdir}/setupDatarel-1928-${shost}-slot${cslot}.log`
arr4=( `echo "$nlines4" | tr -s ' ' ' '` )
num4=${arr4[0]}

c4=1
while [ $num4 -gt 0 ]
do
        c4++
        sleep 30
        sleep ${delay}
        setup -r . > ${logdir}/setupDatarel-1928-${shost}-${c4}-slot${cslot}.log 2>&1

        nlines4=`wc -l ${logdir}/setupDatarel-1928-${shost}-${c4}-slot${cslot}.log`
        arr4=( `echo "$nlines4" | tr -s ' ' ' '` )
        num4=${arr4[0]}
done

cd ${HERE}



#  export LD_PRELOAD=/work/00342/ux453102/interpose/interposed_functions.so


# if [ $num2 > 0 ]; then
#         echo "Not zero: redo Datarel"
# fi


##   eups list > ${rundir}/eups.list

## sleep 60 

## date 



echo "===================== Prior to W2012Pipe "
date

# --input /scratch/00342/ux453102/base/lsst/pt1_2/data/obs/ImSim 

# $PIPE_TASKS_DIR/bin/ processCcdLsstSim.py
# --input /scratch/00342/ux453102/datarel-runs/w2012prod_im0138/output
# --output /scratch/00342/ux453102/datarel-runs/w2012prod_im0138/output
# --id visit=888382340 raft=2,1 sensor=0,2 > logs/W2012Pipe-${visit}.log 2>&1

modstring=`echo ${visit_raft_sensor} | sed -e 's/ /:/g' -e 's/=/-/g' -e 's/,/_/g'`

echo visit_raft_sensor
echo ${visit_raft_sensor}
echo modstring
echo $modstring

$PIPE_TASKS_DIR/bin/processCcdLsstSim.py lsstSim ${rundir}/output --output ${rundir}/output --id ${visit_raft_sensor}  > logs/W2012Pipe-${modstring}.log 2>&1

echo "===================== After W2012Pipe "
date

