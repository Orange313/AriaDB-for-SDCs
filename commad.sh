./compile.sh


echo "=============================="
echo "编译成功"
echo "执行TPCC测试， 协议为Aria..."
echo "=============================="


./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=1 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=1 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=1 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=1 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=1 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=2 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=2 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=2 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=2 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=2 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=4 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=4 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=4 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=4 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=4 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=6 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=6 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=6 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=6 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=6 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=8 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=8 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=8 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=8 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=8 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=10 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=10 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=10 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=10 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=10 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=12 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=12 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=12 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=12 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=12 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=36 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=36 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=36 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=36 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=36 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=60 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=60 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=60 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=60 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=60 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=84 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=84 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=84 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=84 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=84 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=108 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=108 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=108 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=108 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=108 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=132 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=132 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=132 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=132 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=132 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=156 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=156 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=156 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=156 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=156 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:25" --protocol=Aria --partition_num=180 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:26" --protocol=Aria --partition_num=180 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:27" --protocol=Aria --partition_num=180 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:28" --protocol=Aria --partition_num=180 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15
# ./bench_tpcc --logtostderr=1 --id=0 --servers="192.168.19.40:29" --protocol=Aria --partition_num=180 --threads=2 --batch_size=500 --query=mixed --neworder_dist=10 --payment_dist=15