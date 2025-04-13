from flask import Flask, request, jsonify
import requests
from datetime import datetime, timedelta
import re # 导入正则表达式模块，用于更方便地提取数据

app = Flask(__name__)

NASA_HORIZONS_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"

# --- 辅助函数：解析 Horizons API 返回的 result 字符串 ---
def parse_horizons_result(result_string):
    """
    解析 Horizons API 返回的 result 字符串，提取 RA 和 DEC。
    QUANTITIES='1' 返回的格式通常是:
    ' YYYY-Mon-DD HH:MM:SS.fff     R.A._(ICRF/J2000.0)_DEC.    dRA*cosD d(DEC)/dt ...'
    我们需要找到 $$SOE 和 $$EOE 之间的行，并提取第二和第三个字段。
    """
    try:
        # 找到星历数据的开始和结束标记
        start_marker = "$$SOE"
        end_marker = "$$EOE"
        start_index = result_string.find(start_marker)
        end_index = result_string.find(end_marker)

        if start_index == -1 or end_index == -1:
            print("错误：在 Horizons API 结果中未找到 $$SOE 或 $$EOE 标记。")
            return None, None # 或者返回其他表示错误的值

        # 提取星历数据部分 (在 $$SOE 和 $$EOE 之间)
        ephemeris_data = result_string[start_index + len(start_marker):end_index].strip()

        # 按行分割，并处理第一行数据（因为我们只请求了一天的数据）
        lines = ephemeris_data.splitlines()
        if not lines:
            print("错误：$$SOE 和 $$EOE 之间没有数据行。")
            return None, None

        data_line = lines[0].strip() # 获取第一行数据

        # 使用更灵活的分割方式，考虑到可能有多个空格
        # 正则表达式 \s+ 匹配一个或多个空白字符
        # 我们需要日期时间、RA、DEC 这三个主要部分
        # 示例行: ' 2025-Jan-01 00:00:00.0000  19 34 48.70 +30 34 12.8 ...' (HMS/DMS format)
        # 或者 ' 2025-Jan-01 00:00:00.0000  293.70292  30.57022 ...' (Decimal degrees format)
        # API 默认返回 HMS/DMS，QUANTITIES='1' 应该是指 Astrometric RA/DEC
        # 我们需要仔细检查返回的具体格式。

        # 尝试更简单的基于位置的提取，假设字段由多个空格分隔
        # 先移除行首的时间戳部分
        match = re.search(r'\d{4}-\w{3}-\d{2}\s+\d{2}:\d{2}:\d{2}(\.\d+)?\s+(.*)', data_line)
        if not match:
            print(f"错误：无法从行 '{data_line}' 中解析出日期时间后的数据。")
            return None, None

        remaining_data = match.group(2).strip() # 获取时间戳之后的部分

        # 分割剩余部分，RA 和 DEC 应该是前两个（或几组）数字
        parts = re.split(r'\s+', remaining_data)

        if len(parts) < 2: # 需要至少两个部分 (RA 和 DEC)
             print(f"错误：解析出的部分不足以提取 RA 和 DEC: {parts}")
             return None, None

        # 根据 QUANTITIES='1' (Astrometric RA/DEC), 返回的格式通常是
        # RA (HH MM SS.fff) 和 DEC (sDD MM SS.f) 或者直接是度数
        # 为了简化，我们先直接将它们作为字符串返回，让 Unity 端处理或显示
        # 注意：Horizons API 可能返回 HMS.f (时分秒) 或十进制度数格式
        # 这里假设返回的是可以直接使用的字符串表示
        ra = parts[0] # 第一个数据部分认为是 RA
        dec = parts[1] # 第二个数据部分认为是 DEC

        # 如果是 HMS/DMS 格式，RA 可能是3个部分，DEC 是3个部分
        # 例如: '19 34 48.70 +30 34 12.8'
        # 检查DEC部分是否包含符号 (+/-)
        if parts[1].startswith('+') or parts[1].startswith('-'):
            # 极有可能是 HMS/DMS 格式
            if len(parts) >= 6:
                 ra = f"{parts[0]} {parts[1]} {parts[2]}"
                 dec = f"{parts[3]} {parts[4]} {parts[5]}"
            else:
                 # 格式不确定，尝试按原样返回前两个主要部分
                 ra = f"{parts[0]}" # 也许 RA 是度数？
                 dec = f"{parts[1]}" # 也许 DEC 是度数？
                 print(f"警告：检测到可能的 HMS/DMS 格式，但部分数量不足 ({len(parts)})。按前两部分提取 RA='{ra}', DEC='{dec}'")
        elif len(parts) >= 2:
             # 可能是十进制度数格式
             ra = parts[0]
             dec = parts[1]
        else:
             print(f"错误：无法确定 RA/DEC 格式或部分不足：{parts}")
             return None, None


        print(f"解析成功: RA='{ra}', DEC='{dec}'")
        return ra, dec

    except Exception as e:
        print(f"解析 Horizons result 时发生异常: {e}")
        print(f"原始 result 字符串片段: {result_string[:500]}...") # 打印部分原始字符串帮助调试
        return None, None

# --- 辅助函数：从 result 字符串中提取天体名称 ---
def parse_target_name(result_string):
    """尝试从 Horizons API 返回的 result 字符串的头部提取目标天体名称"""
    try:
        lines = result_string.splitlines()
        for line in lines:
            line = line.strip()
            if line.startswith("Target body name:"):
                # 提取 "Target body name:" 后面的部分，并去除可能的编号和空格
                name_part = line.split(":", 1)[1].strip()
                # 移除名称后面的括号和编号，例如 "Venus (299)" -> "Venus"
                name = re.sub(r'\s*\(\d+\)\s*$', '', name_part).strip()
                return name
            # 增加对 JPL Horizons 新格式的兼容
            if line.startswith("Target Body"):
                name_part = line.split(":", 1)[1].strip()
                name = re.sub(r'\s*\(\d+\)\s*$', '', name_part).strip()
                return name
        return "Unknown (解析失败)" # 如果没找到
    except Exception as e:
        print(f"解析目标名称时出错: {e}")
        return "Unknown (异常)"


# --- Flask 路由 ---
@app.route("/get_positions", methods=["GET"])
def get_positions():
    # 定义要查询的天体编号，排除地球（编号 399）
    # 使用明确的大行星和太阳编号
    # 1=Mercury, 2=Venus, 3=EMB, 4=Mars, 5=Jupiter, 6=Saturn, 7=Uranus, 8=Neptune, 9=Pluto, 10=Sun
    # 注意：JPL有时会用 199, 299, 399, 301, 499 等编号
    # 我们先用简单的 1-10 编号尝试，这在很多情况下有效
    bodies = ["1", "2", "4", "5", "6", "7", "8", "9", "10"] # 水金火木土天海冥 + 太阳 (排除了 3 地月系质心)

    lon = request.args.get("lon", default="120.0")  # 经度
    lat = request.args.get("lat", default="30.0")   # 纬度
    elevation = request.args.get("elevation", default="0")  # 海拔 (米)
    time_str = request.args.get("time", default="2025-01-01T00:00:00")  # 时间字符串

    # 重要：Horizons API 对 SITE_COORD 的格式要求是 'lon,lat,elev' (以度为单位的经纬度和以千米为单位的海拔)
    # 或者使用 'geo' 关键字来表示使用大地坐标（经纬度海拔）
    # 根据文档，使用 'geo' 通常更简单可靠
    # site_coord = f"{lon},{lat},{elevation}" # 直接传递经纬度和海拔（单位需确认，Horizons可能期望海拔单位是km）
    # 改为推荐的 'geo' 方式:
    center_coord = f"geo@{lon},{lat},{elevation}" # 不确定API是否支持@符号直接传递，先尝试标准格式

    # 确认 SITE_COORD 参数格式。文档建议使用 CENTER='coord@<id>' 和 COORD_TYPE='GEODETIC'
    # 或者 CENTER='geo' 并提供 SITE_COORD
    # 我们将使用 CENTER='coord@399' (观测站位于地球上) 并提供 SITE_COORD
    # 注意：文档中 SITE_COORD 的海拔单位是 千米(km)！
    try:
        elevation_km = float(elevation) / 1000.0
    except ValueError:
        print(f"警告：无法将海拔 '{elevation}' 转换为数字，将使用 0 km。")
        elevation_km = 0.0
    site_coord_str = f"'{lon},{lat},{elevation_km}'" # 确保海拔是千米

    # 计算 start_time 和 stop_time (保持不变)
    try:
        start_time_dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
        # 为了确保获得该精确时间点的数据，将停止时间设为同一时刻或稍后一点点，步长设为更精细
        # 或者保持1天步长，因为我们只取第一行数据
        start_time = time_str
        stop_time = (start_time_dt + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S") # 停止时间设为1分钟后，确保包含开始时间
        step_size = "1" # 请求单个时间点，或者用 '1m' 步长？ 用 '1d' 配合解析第一行也可以。
                        # 尝试只请求一个点
        step_size = "1" # 设置为 "1" 表示仅计算 START_TIME 这一个点

    except ValueError:
        return jsonify({"error": f"无效的时间格式: '{time_str}'. 需要 'YYYY-MM-DDTHH:MM:SS'"}), 400


    positions = []  # 用于存储返回的天体名称和坐标数据

    print(f"开始查询时间: {start_time}, 观测点: lon={lon}, lat={lat}, elev_km={elevation_km}")

    for body in bodies:
        # 构建每个天体的查询参数
        params = {
            "format": "json",
            "COMMAND": body,
            "OBJ_DATA": "NO", # 关闭物体信息，只获取星历
            "MAKE_EPHEM": "YES", # 明确要求生成星历
            "EPHEM_TYPE": "OBSERVER",
            #"CENTER": f"coord@399", # 指定观测中心在地球(399)上，并使用大地坐标
            "CENTER": f"@{lon},{lat},{elevation_km}", # 尝试直接指定坐标作为中心，需要API支持
            #"COORD_TYPE": "GEODETIC", # 如果使用 coord@399 则需要这个
            "START_TIME": start_time,
            "STOP_TIME": stop_time, # 如果 STEP_SIZE="1", STOP_TIME 会被忽略
            "STEP_SIZE": step_size, # 请求单个时间点
            "QUANTITIES": "1",  # 1 = Astrometric RA/DEC
            "CSV_FORMAT": "NO", # 确保不是CSV格式，即使在result字符串里
            "CAL_FORMAT": "CAL", # 日期格式
            "ANG_FORMAT": "DEG", # **** 请求角度单位为十进制度数 **** 这样更容易解析
            "APPARENT": "AIRLESS" # 使用大气修正前的视位置 (可选)
        }
        # 如果使用 CENTER='coord@399' 的方式:
        # params["CENTER"] = 'coord@399'
        # params["COORD_TYPE"] = 'GEODETIC'
        # params["SITE_COORD"] = site_coord_str # 提供大地坐标

        # 使用直接坐标作为中心（更新的API可能支持）
        # params["CENTER"] = f"'{site_coord_str}'" # 尝试带引号
        params["CENTER"] = f"geo" # 使用 'geo' 并隐式使用SITE_COORD? 不确定，文档有点模糊
                                   # 回退到最常用的方式: CENTER='geo' + SITE_COORD
        params["CENTER"] = "geo"
        params["SITE_COORD"] = site_coord_str # 确保海拔是km

        print(f"\n正在查询天体: {body}")
        # print(f"请求参数: {params}") # 打印参数用于调试

        try:
            response = requests.get(NASA_HORIZONS_URL, params=params)
            response.raise_for_status() # 检查 HTTP 错误 (如 400, 404, 500)

            data = response.json()
            # print(f"原始 JSON 响应: {data}") # 打印原始JSON用于调试

            # 检查 Horizons API 是否在 JSON 结果中直接报告了错误
            if "error" in data:
                print(f"Horizons API 错误 (天体 {body}): {data['error']}")
                continue # 跳过这个天体

            # 检查核心的 'result' 字段是否存在
            if "result" not in data:
                print(f"错误：Horizons API 响应中缺少 'result' 字段 (天体 {body})。响应: {data}")
                continue

            result_string = data['result']
            # print(f"--- Horizons Result String (天体 {body}) ---")
            # print(result_string) # 打印 result 字符串帮助调试解析逻辑
            # print(f"--- End Result String ---")


            # 解析 result 字符串获取 RA 和 DEC
            ra, dec = parse_horizons_result(result_string)

            # 解析目标名称
            body_name = parse_target_name(result_string)

            if ra is not None and dec is not None:
                print(f"成功提取数据: Name={body_name}, RA={ra}, DEC={dec}")
                # 存储简化后的信息
                positions.append({
                    "name": body_name, # 使用从结果中解析出的名称
                    "ra": ra,
                    "dec": dec
                })
            else:
                print(f"未能从天体 {body} 的结果中提取 RA/DEC。")


        except requests.exceptions.RequestException as e:
            print(f"请求 Horizons API 时发生网络错误 (天体 {body}): {e}")
            # 这里可以选择是继续下一个天体还是直接返回错误
            # return jsonify({"error": f"请求 NASA API 失败: {e}"}), 500 # 或者直接失败
            continue # 尝试下一个天体
        except ValueError as e: # 处理 response.json() 可能的错误
             print(f"解析 Horizons API 响应 JSON 时失败 (天体 {body}): {e}")
             print(f"服务器原始响应文本: {response.text[:500]}...") # 打印部分原始文本
             continue
        except Exception as e:
            print(f"处理天体 {body} 时发生未知错误: {e}")
            import traceback
            traceback.print_exc() # 打印完整的错误堆栈
            continue # 尝试下一个天体

    print(f"\n查询完成，共获取到 {len(positions)} 个天体的位置。")
    return jsonify(positions) # 返回包含所有成功获取位置的列表

if __name__ == "__main__":
    # 运行 Flask 开发服务器
    # debug=True 会在代码更改时自动重载，并提供更详细的错误页面
    # host='0.0.0.0' 使服务器可以从网络中的其他设备访问（如果防火墙允许）
    # 如果只在本地测试，'127.0.0.1' 或不指定 host 也可以
    app.run(debug=True, host='0.0.0.0', port=5000)