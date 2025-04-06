from tree import build_bplus_tree_from_csv

def main():
    csv_file = "log_with_sdc_03281841.csv"  
    tree = build_bplus_tree_from_csv(csv_file)
    print("B+ 树构造完成！")
    
    #测试单点查询  ds
    test_lsn = 33025
    records = tree.search(test_lsn)
    print(f"查询 LSN={test_lsn} 得到 {len(records)} 条记录：")
    for rec in records:
        print(rec)
    
    #测试范围查询
    # start_lsn, end_lsn = 33024, 33028 #33024-33028查不到
    # range_results = tree.range_search(start_lsn, end_lsn)
    # print(f"范围查询 LSN {start_lsn}~{end_lsn} 得到 {len(range_results)} 条记录：")
    # for key, rec in range_results:
    #     print(f"LSN={key} => {rec}")

if __name__ == "__main__":
    main()
