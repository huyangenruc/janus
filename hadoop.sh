#!/bin/bash



if [ "$1" ];then
   echo "ip address: $1"
else
   echo "please input ip address"
   exit 2
fi

if [ "$2" ];then
   echo "ip address: $2"
else
   echo "please input hostname"
   exit 2
fi

a=`cat /etc/hosts|grep $1|grep $2`
if [ "$a" ];then
   echo "a is :$a ,already exists in hosts"
else
  #  echo "$1            $2">>/etc/hosts
    echo "add ip and hostname to hosts"
fi

b=`cat /usr/local/cloud/hadoop/hadoop-0.20.2-cdh3u6/conf/slaves|grep $2`
echo $b

if [ "$b" ];then
   echo "already exist"
else
   echo "push $2 to hadoop slaves"
   echo "$2">>/usr/local/cloud/hadoop/hadoop-0.20.2-cdh3u6/conf/slaves
fi



# create hadoop user
ssh root@$1 "

adduser hadoop
echo 'hadoop   ALL=(ALL:ALL) ALL'>> /etc/sudoers
mkdir /usr/local/cloud
mkdir /usr/local/cloud/hadoop
mkdir /usr/local/cloud/java
mkdir /usr/local/cloud/vina
mkdir /home/hadoop/.ssh
chown -R hadoop /home/hadoop/.ssh
chown -R hadoop /usr/local/cloud

echo '
export HADOOP_HOME=/usr/local/cloud/hadoop/hadoop-0.20.2-cdh3u6
export VINA_HOME=/usr/local/cloud/vina/autodock_vina_1_1_2_linux_x86
export JAVA_HOME=/usr/local/cloud/java/jdk1.7.0_25
export JRE_HOME=\${JAVA_HOME}/jre
export CLASSPATH=.:\${JAVA_HOME}/lib:\${JRE_HOME}/lib
export PATH=\${JAVA_HOME}/bin:\$PATH:\$HADOOP_HOME/bin:\$PATH:\$VINA_HOME/bin' >> /etc/profile


echo '
192.168.30.26   zhaoyong1
192.168.30.27   zhaoyong2
192.168.30.28   zhaoyong3
192.168.30.29   zhaoyong4
192.168.30.30   zhaoyong5
192.168.30.32   zhaoyong6
192.168.30.33   zhaoyong7
192.168.30.34   zhaoyong8
192.168.30.36   zhaoyong9
192.168.30.37   zhaoyong10
192.168.30.38   zhaoyong11
192.168.30.39   zhaoyong12
192.168.30.40   zhaoyong13
192.168.30.41   zhaoyong14
192.168.30.42   zhaoyong15
192.168.30.43   zhaoyong16
192.168.30.44   zhaoyong17
192.168.30.45   zhaoyong18
192.168.30.46   zhaoyong19
192.168.30.47   zhaoyong20
192.168.30.48   zhaoyong21
192.168.30.49   zhaoyong22
192.168.30.50   zhaoyong23
192.168.30.51   zhaoyong24
192.168.30.52   zhaoyong25' >> /etc/hosts

source /etc/profile
" 

scp /home/hadoop/.ssh/authorized_keys hadoop@$2:/home/hadoop/.ssh/
ssh hadoop@$2 "
  echo 'login in hadoop'
" 

# '$1        $2'>>/etc/hosts 
scp /usr/local/cloud/hadoop/hadoop-0.20.2-cdh3u6.tar.gz hadoop@$2:/usr/local/cloud/hadoop/
scp /usr/local/cloud/java/jdk-7u25-linux-x64.tar.gz hadoop@$2:/usr/local/cloud/java
scp /usr/local/cloud/vina/autodock_vina_1_1_2_linux_x86.tgz hadoop@$2:/usr/local/cloud/vina

#tar file 
ssh hadoop@$2 "
cd /usr/local/cloud/hadoop/
tar xvf /usr/local/cloud/hadoop/hadoop-0.20.2-cdh3u6.tar.gz 

cd /usr/local/cloud/java/
tar xvf /usr/local/cloud/java/jdk-7u25-linux-x64.tar.gz

cd /usr/local/cloud/vina/
tar xvf /usr/local/cloud/vina/autodock_vina_1_1_2_linux_x86.tgz
source /etc/profile
jps
"


scp -r /usr/local/cloud/hadoop/hadoop-0.20.2-cdh3u6/conf/  hadoop@$2:/usr/local/cloud/hadoop/hadoop-0.20.2-cdh3u6/
scp -r /usr/local/cloud/hadoop/hadoop-0.20.2-cdh3u6/bin/  hadoop@$2:/usr/local/cloud/hadoop/hadoop-0.20.2-cdh3u6/


ssh hadoop@$2 "
source /etc/profile
hadoop-daemon.sh start datanode
hadoop-daemon.sh start tasktracker
jps
"


if [ `whoami` == "hadoop"  ];then 
    echo "hadoop" 
else
     echo "not hadoop"  
fi
