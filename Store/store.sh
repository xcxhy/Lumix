###
 # @Author: xuhao0101
 # @Date: 2023-08-21 03:35:32
 # @LastEditTime: 2023-08-21 03:38:40
 # @LastEditors: localhost04
 # @Description: In User Settings Edit
 # @FilePath: /Focus_Dataset_v2/store.sh
### 
#!/bin/bash

cd /home/xuhao/xcxhy/Focus_Dataset_v2

for (( i=1 ; i<=19 ; i+=1 ))
do 
    python store.py
    echo $i
done