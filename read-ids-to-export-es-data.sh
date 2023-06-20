#!/bin/bash

# 读取ids数据文件
ids_file=$1

if [ "$ids_file" == "" ]; then
  echo "Usage：$0 <ids_file>"
  exit 1
fi

# 每次读取的数据量
read_size=200

# 文件名
msgs_file="msgs.data"

# 总行数
total_rows=$(wc -l < "$ids_file")

# 总页数
total_pages=$((total_rows%read_size==0 ? total_rows/read_size : total_rows/read_size+1))


# 导出es数据
function export_es_data() {

  ids=$1
  es_host="127.0.0.1:9200"
  es_account="username:password"
  es_index="data_index_202306"
  filter_path="aggregations.group_by_type.buckets.latest_date_time.hits.hits._source"

  curl -XGET -H 'Content-Type: application/json' -u $es_account http://$es_host/$es_index/_search?filter_path=$filter_path -d '
  {
    "size": 0,
    "query": {
      "terms": {
        "device_id": [
          '"$ids"'
        ]
      }
    },
    "aggs": {
      "group_by_type": {
        "terms": {
          "size": '"$read_size"',
          "field": "device_id.keyword"
        },
        "aggs": {
          "latest_date_time": {
            "top_hits": {
              "sort": [
                {
                  "collect_time.keyword": {
                    "order": "desc"
                  }
                }
              ],
              "size": 1,
              "_source": {
                "excludes": [
                  "device_status"
                ]
              }
            }
          }
        }
      }
    }
  }
  ' | jq -c '.aggregations.group_by_type.buckets[].latest_date_time.hits.hits[]._source' >> "$msgs_file"
  
  # 读取每个对象并分别压缩成一行数据分别存入文件
  # jq -c '.aggregations.group_by_type.buckets[].latest_date_time.hits.hits[]._source' >> msgs.data

  # 读取每个对象并将其以[]数组的形式存入json文件
  # jq '[.aggregations.group_by_type.buckets[].latest_date_time.hits.hits[]._source]' > msgs.json

}


#初始化数据文件
echo > "$msgs_file"


# 循环打印文本内容并分页显示
for ((page=1; page<=total_pages; page++)); do

  # 计算起始行(偏移量)
  start_rows=$(( (page - 1) * read_size + 1 ))

  # 分页读取数据(初始位置|记录数|除去末尾逗号)
  ids=$(tail -n +$start_rows "$ids_file" | head -n $read_size | sed '$s/,$//')

  # 打印提示
  echo -e "\nIds total rows: $total_rows, total pages: $total_pages, currently reading page $page..."

  # 导出es数据
  export_es_data "$ids"

done


# 数据行数
msgs_rows=$(wc -l < "$msgs_file")

# 数据大小
msgs_size=$(ls -lh "$msgs_file" | awk '{print $5}')

# 打印提示
echo -e "\n  Request completed. msgs total rows: $msgs_rows, msgs total size: $msgs_size. \n"
