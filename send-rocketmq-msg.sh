#!/bin/bash

# 读取msg数据文件
msgs_file=$1

if [ "$msgs_file" == "" ]; then
  echo "Usage：$0 <msgs_file>"
  exit 1
fi

#读取json数组,并将每个对象压缩为单行数据
#msg=($(cat msgs.json | jq -c '.[]'))

#读取压缩处理过的数据(一行一个json对象)
#msgs=($(cat msgs.data))

msgs=($(cat "$msgs_file"))

for msg in "${msgs[@]}"
do
 echo "$msg"
 sh /usr/rocketmq/bin/mqadmin sendMessage -n 127.0.0.1:9876 -t COLLECTION_DATA_PERCEPTION_TOPIC -p "$msg"
 echo -e "--------------------------------\n"
done
