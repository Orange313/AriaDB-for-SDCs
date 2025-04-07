from tree import build_bplus_tree_from_csv
import csv

def main():
    csv_file = "log1_with_sdc_33021.csv"  
    tree = build_bplus_tree_from_csv(csv_file)
    tree.fix_leaf_chain()
    print("B+ 树构造完成！")
    
    #测试单点查询  ds
    # test_lsn = 33025
    # records = tree.robust_search(test_lsn)
    # print(f"查询 LSN={test_lsn} 得到 {len(records)} 条记录：")
    # for rec in records:
    #     print(rec)
    
    # test_lsn = 33025
    # records = tree.search(test_lsn)
    # print(f"查询 LSN={test_lsn} 得到 {len(records)} 条记录：")
    # for rec in records:
    #     print(rec)

    #测试范围查询
    start_lsn, end_lsn = 1, 33021 #33025-65793查不到
    range_results = tree.range_search(start_lsn, end_lsn)
    print(f"范围查询 LSN {start_lsn}~{end_lsn} 得到 {len(range_results)} 条记录：")
    for key, rec in range_results:
        print(f"LSN={key} => {rec}")
    # output_file = "range_query_results.csv"
    # with open(output_file, mode='w', newline='') as csvfile:
    #     writer = csv.writer(csvfile)
    #     writer.writerow(["LSN"])
    #     for key, _ in range_results:
    #         writer.writerow([key])

    # print(f"范围查询结果已写入 {output_file}，共 {len(range_results)} 条记录。")


if __name__ == "__main__":
    main()
