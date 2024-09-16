#!/bin/sh

set -o nounset  # Thoát script nếu có biến chưa được định nghĩa
set -o errexit  # Thoát script nếu có lệnh thất bại

# Đảm bảo đúng số lượng tham số được cung cấp
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <file>"
    exit 1
fi

file="$1"
echo "Starting to send data from $file"

network_port=9999
lines_in_batch=100
interval_sec=10

# Kiểm tra sự tồn tại của file
if [ ! -f "$file" ]; then
    echo "File not found: $file"
    exit 1
fi

n_lines=$(wc -l < "$file")
echo "Total lines in file: $n_lines"

cursor=1

while [ $cursor -le $n_lines ]; do
    # Gửi các dòng theo từng batch
    tail -n +"$cursor" "$file" | head -n $lines_in_batch | nc -q 0 localhost $network_port
    
    if [ $? -ne 0 ]; then
        echo "Error sending data to port $network_port"
        exit 1
    fi

    echo "Sent lines $cursor to $(($cursor + $lines_in_batch - 1))"
    
    cursor=$(($cursor + $lines_in_batch))
    sleep $interval_sec
done

echo "Finished sending data from $file"
