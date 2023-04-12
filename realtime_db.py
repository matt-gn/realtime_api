from urllib.request import urlopen
from urllib.error import HTTPError
from datetime import datetime
from sqlite3 import connect

argos = (
    (8909, 'Cape Denison', [160, 185], 'Adelie Coast'),
    (8914, 'D-10', [165, 182], 'Adelie Coast'),
    (8916, 'D-47', [165, 180], 'Adelie Coast'),
    (8912, 'D-85', [165, 175], 'Adelie Coast'),
    (8927, 'AGO-4', [155, 150], 'High Polar Plateau'),
    (8989, 'Dome C II', [160, 165], 'High Polar Plateau'),
    (8904, 'Dome Fuji', [142, 125], 'High Polar Plateau'),
    (8985, 'Henry', [134, 140], 'High Polar Plateau'),
    (30305, 'JASE2007', [145, 115], 'High Polar Plateau'),
    (21359, 'Mizuho', [162, 115], 'High Polar Plateau'),
    (8924, 'Nico', [140, 145], 'High Polar Plateau'),
    (8918, 'Relay Station', [155, 120], 'High Polar Plateau'),
    (8984, 'Possession Island', [140, 185], 'Ocean Islands'),
    (8988, 'Whitlock', [137, 177], 'Ocean Islands'),
    (8905, 'Manuela', [142, 180], 'Reeves Glacier'),
    (21357, 'Elaine', [137, 160], 'Ross Ice Shelf'),
    (8939, 'Emilia', [137, 167], 'Ross Ice Shelf'),
    (8919, 'Emma', [131, 161], 'Ross Ice Shelf'),
    (8911, 'Gill', [130, 165], 'Ross Ice Shelf'),
    (8928, 'Lettau', [132, 162], 'Ross Ice Shelf'),
    (8910, 'Margaret', [127, 162], 'Ross Ice Shelf'),
    (8934, 'Marilyn', [140, 160], 'Ross Ice Shelf'),
    (8915, 'Sabrina', [130, 160], 'Ross Ice Shelf'),
    (8913, 'Schwerdtfeger', [137, 162], 'Ross Ice Shelf'),
    (8931, 'Vito', [135, 170], 'Ross Ice Shelf'),
    (8947, 'Ferrell', [137, 169], 'Ross Island'),
    (21360, 'Laurie II', [137, 171], 'Ross Island'),
    (8906, 'Marble Point', [145, 175], 'Ross Island'),
    (7351, 'Alessandra (IT)', [144, 180], 'Transantarctic Mountains'),
    (7357, 'Arelis (IT)', [145, 175], 'Transantarctic Mountains'),
    (7353, 'Eneide (IT)', [144, 180], 'Transantarctic Mountains'),
    (7355, 'Modesta (IT)', [147, 179], 'Transantarctic Mountains'),
    (7354, 'Rita (IT)', [145, 177], 'Transantarctic Mountains'),
    (7350, 'Sofia (IT)', [150, 180], 'Transantarctic Mountains'),
    (8903, 'Byrd', [115, 152], 'West Antarctica'),
    (21361, 'Elizabeth', [120, 155], 'West Antarctica'),
    (21363, 'Erin', [125, 150], 'West Antarctica'),
    (8900, 'Harry', [117, 150], 'West Antarctica'),
    (8936, 'Janet', [115, 160], 'West Antarctica'),
    (30393, 'Siple Dome', [125, 160], 'West Antarctica'),
    (8930, 'Thurston Island', [97, 150], 'West Antarctica')
)

def get_data_url(argos_id: str):
    return f"https://amrc.ssec.wisc.edu/data/surface/awstext/{argos_id}.txt"

def read_data(url: str) -> list | None:
    data = urlopen(url).read().decode('utf-8').strip().split('\n')
    table = [line.split()[1:] for line in data if len(line.split()) == 10][2:]
    if len(table) > 0:
        return table[-1]
    else:
        return None

def process_datapoint(station_name: str, coords: list, region: str, data: list) -> tuple:
    try:
        date_str, time, temp, press, wind_spd, wind_dir, hum, _, _ = data
        formatted_date_str = datetime.strptime(date_str, '%Y%j')
        formatted_time_str = datetime.strptime(time, '%H%M%S')
        standard_date = formatted_date_str.strftime('%Y-%m-%d')
        standard_time = formatted_time_str.strftime('%H:%M:%S')
        params = (
            station_name,
            standard_date,
            standard_time,
            float(temp),
            float(press),
            float(wind_spd),
            int(wind_dir),
            float(hum),
            coords[0],
            coords[1],
            region
        )
        return params
    except:
        return None

def build_realtime_table() -> None:
    with connect("/usr/local/realtime_api/realtime.db") as connection:
        connection.execute("""CREATE TABLE aws_realtime(station_name, date, time, temperature, pressure, wind_speed,
                           wind_direction, humidity, latitude, longitude, region)""")
        for (aws, station_name, coords, region) in argos:
            data = read_data(get_data_url(aws))
            if data:
                params = process_datapoint(station_name, coords, region, data)
                if params:
                    connection.execute("INSERT INTO aws_realtime VALUES(?,?,?,?,?,?,?,?,?,?,?)", params)
        connection.commit()

def update_realtime_table() -> None:
    with connect("/usr/local/realtime_api/realtime.db") as connection:
        for (aws, station_name, coords, region) in argos:
            data = read_data(get_data_url(aws))
            if data:
                params = process_datapoint(station_name, coords, region, data)
                connection.execute("""UPDATE aws_realtime
                                    SET station_name=?, date=?, time=?, temperature=?,
                                    pressure=?, wind_speed=?, wind_direction=?, humidity=?,
                                    latitude=?, longitude=?, region=? WHERE station_name=?""",
                                    params + (station_name,))
        connection.commit()

def build_aggregate_table():
    with connect("/usr/local/realtime_api/realtime.db") as connection:
        connection.execute("CREATE TABLE daily_aggregate_table(date, time, agg_type, variable, station_name, datapoint)")
        for variable in ("temperature", "pressure", "wind_speed", "wind_direction", "humidity"):
            # MAX
            current_max = connection.execute(f"SELECT date, time, station_name, max({variable}) FROM aws_realtime").fetchall()[0]
            insert_row = (current_max[0], current_max[1], "max", variable, current_max[2], current_max[3])
            connection.execute("INSERT INTO daily_aggregate_table VALUES(?, ?, ?, ?, ?, ?)", insert_row)
            # MIN
            current_min = connection.execute(f"SELECT date, time, station_name, min({variable}) FROM aws_realtime").fetchall()[0]
            insert_row = (current_min[0], current_min[1], "min", variable, current_min[2], current_min[3])
            connection.execute("INSERT INTO daily_aggregate_table VALUES(?, ?, ?, ?, ?, ?)", insert_row)
            # AVG
            current_avg = connection.execute(f"SELECT date, round(avg({variable}), 2) FROM aws_realtime").fetchall()[0]
            insert_row = (current_avg[0], "0", "avg", variable, "None", current_avg[1])
            connection.execute("INSERT INTO daily_aggregate_table VALUES(?, ?, ?, ?, ?, ?)", insert_row)
        connection.commit()

def update_aggregate_table():
    with connect("/usr/local/realtime_api/realtime.db") as connection:
        ## daily_aggregate_table should have 15 ROWS total at all times
        ## DATE     TIME    AGG_TYPE    VARIABLE        STATION_NAME    DATAPOINT
        ## 2/3/23   012345  max         temperature     Byrd            -01.12
        ## 2/3/23   123455  min         temperature     Byrd            -23.02
        ## 2/3/23   000000  avg         temperature     None            -32.22
        daily_agg = connection.execute("SELECT * FROM daily_aggregate_table").fetchall()
        for variable in ("temperature", "pressure", "wind_speed", "wind_direction", "humidity"):
            current_max = connection.execute(f"SELECT date, time, station_name, max({variable}) FROM aws_realtime").fetchall()[0]
            current_min = connection.execute(f"SELECT date, time, station_name, min({variable}) FROM aws_realtime").fetchall()[0]
            current_avg = connection.execute(f"SELECT date, round(avg({variable}), 2) FROM aws_realtime").fetchall()[0]
            daily_max = [item for row in daily_agg if row[2] == "max" and row[3] == variable for item in row]
            daily_min = [item for row in daily_agg if row[2] == "min" and row[3] == variable for item in row]
            daily_avg = [item for row in daily_agg if row[2] == "avg" and row[3] == variable for item in row]
            if float(current_max[3]) > float(daily_max[5]) or current_max[0] != daily_max[0]:
                update_row = (current_max[0], current_max[1], current_max[2], current_max[3], "max", variable)
                connection.execute("""UPDATE daily_aggregate_table SET date=?, time=?, station_name=?, datapoint=?
                                   WHERE agg_type=? AND variable=?""", update_row)
            if float(current_min[3]) < float(daily_min[5]) or current_min[0] != daily_min[0]:
                update_row = (current_min[0], current_min[1], current_min[2], current_min[3], "min", variable)
                connection.execute("""UPDATE daily_aggregate_table SET date=?, time=?, station_name=?, datapoint=?
                                   WHERE agg_type=? AND variable=?""", update_row)
            if current_avg[0] == daily_avg[0]:
                new_avg = round(((float(current_avg[1]) + float(daily_avg[5])) / 2), 2)
                update_row = (current_avg[0], new_avg, "avg", variable)
                connection.execute("""UPDATE daily_aggregate_table SET date=?, datapoint=?
                                    WHERE agg_type=? AND variable=?""", update_row)
            else:
                update_row = (current_avg[0], float(current_avg[1]), "avg", variable)
                connection.execute("""UPDATE daily_aggregate_table SET date=?, datapoint=?
                                    WHERE agg_type=? AND variable=?""", update_row)
        connection.commit()

def init() -> None:
    build_realtime_table()
    build_aggregate_table()

def main() -> None:
    update_realtime_table()
    update_aggregate_table()

if __name__ == "__main__":
    main()

