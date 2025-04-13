import sqlite3
import pandas as pd
import os

# --- 配置 ---
# ★★★ 修改为你【第三份表格】对应的 Excel 文件路径 ★★★
XLSX_FILE = r'C:\Users\25906\Desktop\2\1.xlsx' # <--- 请务必修改为实际路径和文件名！

# ★★★ 修改为你想要的 SQLite 数据库文件名 ★★★
DB_FILE = 'historical_records_v3.db' # 可以使用新名称以区分
# SQLite 表名
TABLE_NAME = 'historical_records' # 表名可以保持不变

# *** 更新的列映射 (根据第三张图) ***
# ★★★ 确保这里的 *键* 与你新 Excel 文件中的实际标题完全匹配 ★★★
COLUMN_MAPPING = {
    '年号干支纪年': 'reign_year_ganzhi',
    '公元纪年': 'gregorian_date_text',       # 保持 TEXT 类型以处理潜在空值/文本
    '大气现象记录': 'phenomenon_description', # 确认此名称与 Excel 标题一致
    '史料来源': 'source',                   # 改回 source (因为没有 source_2)
    '备注': 'remarks'
    # '史料来源2' 已移除
}

# --- 函数定义 ---

def create_connection(db_file):
    """ 创建数据库连接 """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(f"连接数据库时出错: {e}")
    return conn

def drop_and_create_table(conn):
    """ 删除旧表 (如果存在) 并根据【第三张图】的结构创建表 """
    sql_drop_table = f"DROP TABLE IF EXISTS {TABLE_NAME};"
    # *** 更新后的 CREATE TABLE 语句 (无 source_2, source_1 改为 source) ***
    sql_create_table = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reign_year_ganzhi TEXT NOT NULL UNIQUE,
        gregorian_date_text TEXT NOT NULL,      -- 保持 NOT NULL 约束
        phenomenon_description TEXT,
        source TEXT,                            -- 列名改为 source
        remarks TEXT
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql_drop_table)
        cursor.execute(sql_create_table)
        conn.commit()
        print(f"表 '{TABLE_NAME}' 已成功根据新结构重新创建。")
    except sqlite3.Error as e:
        print(f"删除或创建表时出错: {e}")


def import_data(conn, xlsx_file):
    """ 从 XLSX 文件读取数据，去重、移除空日期行后导入数据库 """
    if not os.path.exists(xlsx_file):
        print(f"错误: Excel 文件未找到 '{xlsx_file}'")
        return

    try:
        print(f"正在从 '{xlsx_file}' 读取数据...")
        df = pd.read_excel(xlsx_file, engine='openpyxl')
        print(f"成功从 '{xlsx_file}' 读取了 {len(df)} 行数据。")

        # 检查必需的列是否存在 (基于更新的 COLUMN_MAPPING)
        expected_columns = list(COLUMN_MAPPING.keys())
        actual_columns = list(df.columns)
        missing_cols = [col for col in expected_columns if col not in actual_columns]

        if missing_cols:
            print(f"错误: Excel 文件缺少必需的列: {', '.join(missing_cols)}")
            print(f"期望的列: {expected_columns}")
            print(f"实际找到的列: {actual_columns}")
            return

        # 选择并重命名列
        df_selected = df[expected_columns].rename(columns=COLUMN_MAPPING)
        rows_after_rename = len(df_selected)
        print(f"重命名后，有 {rows_after_rename} 行数据。")

        # 1. 处理 'reign_year_ganzhi' 列的重复值
        print(f"正在基于 'reign_year_ganzhi' 列删除重复行...")
        df_deduplicated = df_selected.drop_duplicates(subset=['reign_year_ganzhi'], keep='first')
        rows_after_dedup = len(df_deduplicated)
        duplicates_removed = rows_after_rename - rows_after_dedup
        if duplicates_removed > 0:
            print(f"已删除 {duplicates_removed} 行 'reign_year_ganzhi' 重复记录。")
        else:
            print("未发现 'reign_year_ganzhi' 重复行。")

        # 2. 处理 'gregorian_date_text' 列的空值 (对应 Excel 的 '公元纪年')
        print(f"当前有 {rows_after_dedup} 行数据。")
        print(f"正在检查并删除 'gregorian_date_text' 列为空的行...")
        df_filtered = df_deduplicated.dropna(subset=['gregorian_date_text'])
        rows_after_dropna = len(df_filtered)
        null_dates_removed = rows_after_dedup - rows_after_dropna
        if null_dates_removed > 0:
            print(f"已删除 {null_dates_removed} 行，因为 'gregorian_date_text' (对应 Excel '公元纪年') 为空。")
        else:
            print("未发现 'gregorian_date_text' 为空的行。")

        # 3. 数据清洗: 将剩余的 Pandas 空值 (NaN) 转换为 None
        df_cleaned = df_filtered.where(pd.notnull(df_filtered), None)

        # --- 导入数据 ---
        if len(df_cleaned) > 0:
            try:
                print(f"正在将 {len(df_cleaned)} 行有效数据导入到表 '{TABLE_NAME}'...")
                # 使用清理和过滤后的 DataFrame: df_cleaned
                df_cleaned.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
                print(f"成功将 {len(df_cleaned)} 行数据导入到表 '{TABLE_NAME}'。")
            except sqlite3.Error as e:
                 print(f"\n数据导入时发生 SQLite 错误: {e}")
                 print("请检查数据是否满足表的其他约束或是否存在其他问题。")
            except Exception as e:
                 print(f"\n使用 to_sql 导入数据时发生未知错误: {e}")
        else:
            print("经过处理后，没有有效数据可导入数据库。")


    except FileNotFoundError:
        print(f"错误: Excel 文件未找到 '{xlsx_file}'")
    except ImportError:
         print("错误: 需要 'pandas' 和 'openpyxl' 库。请运行: pip install pandas openpyxl")
    except Exception as e:
        print(f"读取或处理 Excel 文件时发生未知错误: {e}")


# --- 主程序 ---
if __name__ == '__main__':
    print("脚本开始执行...")
    connection = create_connection(DB_FILE)

    if connection:
        # 每次运行时删除旧表并创建新表
        drop_and_create_table(connection)

        # 从 Excel 导入数据 (包含去重和移除空日期逻辑)
        import_data(connection, XLSX_FILE)

        # 关闭数据库连接
        connection.close()
        print("\n数据库连接已关闭。")
    else:
        print("未能连接到数据库，脚本终止。")

    print("脚本执行完毕。")