import os
from flask import Flask, flash, request, redirect, url_for, jsonify
from flask_mysqldb import MySQL
from werkzeug.utils import secure_filename
import json
import pandas as pd
from pydfs_lineup_optimizer import Site, Sport, get_optimizer, Player, CSVLineupExporter
import sys
import requests
import csv
import gc
from save_scrape_data import *
from decimal import Decimal
from datetime import datetime
from draftfast import rules
from draftfast.optimize import run, run_multi
from draftfast.orm import Player
from draftfast.csv_parse import salary_download, uploaders
from draftfast.settings import OptimizerSettings, Stack
from draftfast.lineup_constraints import LineupConstraints
from nose import tools as ntools
UPLOAD_FOLDER = '/home/johnh/upload/'
ALLOWED_EXTENSIONS = set(['csv'])

static_file_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'static')

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROPAGATE_EXCEPTIONS'] = True

app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'asdf'
app.config['MYSQL_DB'] = 'fsport'

mysql = MySQL(app)

# custom optimizer
def optimizeJSON(payload):
    raw = pd.read_json((payload["raw"]))
    data = pd.read_json((payload["posted"]))
    optimizer = None
    if (payload["site"] == "DRAFTKINGS"):
        if (payload["sport_type"] == "PGA"):
            optimizer = get_optimizer(Site.DRAFTKINGS, Sport.GOLF)
        elif (payload["sport_type"] == "NFL"):
            optimizer = get_optimizer(Site.DRAFTKINGS, Sport.FOOTBALL)
        else:
            optimizer = get_optimizer(Site.DRAFTKINGS, Sport.BASKETBALL)
        raw['AvgPointsPerGame'] = raw[payload["projection"]]
        raw['AvgPointsPerGame'] = pd.to_numeric(raw["AvgPointsPerGame"]).fillna(0)
    else:
        if (payload["sport_type"] != "PGA"):
            optimizer = get_optimizer(Site.FANDUEL, Sport.FOOTBALL)
        if (payload["sport_type"] == "NBA"):
            optimizer = get_optimizer(Site.FANDUEL, Sport.BASKETBALL)
        raw['FPPG'] = raw[payload["projection"]]
        raw['FPPG'] = pd.to_numeric(raw["FPPG"]).fillna(0)
        raw['First Name'] = raw['Name']
        raw['Last Name'] = raw['Name']
        raw['Nickname'] = raw['Name']
        raw["Injury Indicator"] = ""
        raw['Team'] = raw['TeamAbbrev']
    # if os.path.exists("lineup.csv"):
        # os.remove("lineup.csv")
    lineup_writer = open('/home/johnh/lineup.csv', 'w+')
    lineup_writer.close()
    raw.to_csv("/home/johnh/lineup.csv")

    del raw
     
    if payload["site"] == "FANDUEL" and payload["sport_type"] == "PGA":
        
        players = salary_download.generate_players_from_csvs(
            salary_file_location='/home/johnh/lineup.csv',
            game=rules.FAN_DUEL,
        )
        
        iterations=payload["lineups"]
        index = 0
        exposure_players = []
        locked = []
        removed = []

        for r in data["Name"]:
            if (data["Lock"][index] == 0) and (data["Remove"][index] == 0):
                exp_player = {}
                exp_player["name"] = r
                exp_player["min"] = 0.1
                exp_player["max"] = data["Exposure"][index] / 100.0
                exposure_players.append(exp_player)
            if (data["Lock"][index] == 1) and (data["Remove"][index] == 0):
                locked.append(data["Name"][index])
            if (data["Lock"][index] == 0) and (data["Remove"][index] == 1):
                removed.append(data["Name"][index])
            index = index + 1
        
        rosters, exposure_diffs  = run_multi(
            iterations=iterations,
            rule_set=rules.FD_PGA_RULE_SET,
            player_pool=players,
            exposure_bounds=exposure_players,
            constraints=LineupConstraints(
                locked=locked,
                banned=removed
            )
        )
               
        ntools.assert_equal(len(rosters), iterations)
        ntools.assert_equal(len(exposure_diffs), 0)

        lineups = []
        exports = []
        exports.append(["G","G","G","G","G","G","Budget","FPPG"])
        for r in rosters:
            export = []
            for p in r.sorted_players():
                lineup = []
                lineup.append("G")
                lineup.append(p.name)
                lineup.append(p.team)
                lineup.append(p.proj)
                lineup.append(p.cost)
                lineups.append(lineup)
                name_index = list(data["Name"]).index(p.name)
                export.append(data["ID"][name_index] + ":" + p.name)
            total = ["", "", ""]
            total.append("{0:.2f}".format(round(r.projected(),2)))
            total.append(r.spent())
            lineups.append(total)
            exports.append(export)
        
        del lineups
        del exports
        del total
        del iterations
        del exposure_players
        del locked
        del removed

        result = {"lineups": lineups,
                  "export": exports}
        return result

    optimizer.load_players_from_csv("/home/johnh/lineup.csv")

    # advanced options (lock, remove and exposure)
    
    for index, row in data.iterrows():

        if (row['Remove'] == 1):
            print("Removing player: " + row['Name'], row['Remove'])
            player = optimizer.get_player_by_id(row['ID'])
            optimizer.remove_player(player)
        if (row['Lock'] == 1):
            print("Lock player: " + row['Name'], row['Remove'])
            player = optimizer.get_player_by_id(row['ID'])
            optimizer.add_player_to_lineup(player)
        if ((row['Exposure'] >= 0) and (row['Exposure'] < 100)):
            #print("Set exposure : " + row['Name'], row['Exposure'] / 100.0)
            player = optimizer.get_player_by_id(row['ID'])
            #print(player)
            player.max_exposure = row['Exposure'] / 100.0
    if (payload["sport_type"] == "NFL") and (payload["site"] == "DRAFTKINGS"):
        optimizer.set_min_salary_cap(49500)        
    lineupGenerator = optimizer.optimize(payload["lineups"])

    def render_player(player):
        # type: (LineupPlayer) -> str
        result = player.full_name
        #print player.id
        if player.id:
            # if payload["site"] == "FANDUEL" and payload["sport_type"] == "NFL":
                # result = player.id + ":" + result
            # else:
            result += '(%s)' % player.id
        return result

    rows = []
    header = None
    print(lineupGenerator)
    for index, lineup in enumerate(lineupGenerator):
        if index == 0:
            header = [player.lineup_position for player in lineup.lineup]
            header.extend(('Budget', 'FPPG'))
        row = [(render_player)(player) for player in lineup.lineup]
        row.append(str(lineup.salary_costs))
        row.append(str(lineup.fantasy_points_projection))
        rows.append(row)

    data = pd.DataFrame.from_records(rows, columns = header)

    lineups = pd.DataFrame()

    names = []
    positions = []
    teams = []
    fppgs = []
    salaries = []

    # add number to duplicate cols
    def rename_duplicates(old):
        new = []
        seen = {}
        for x in old:
            if x in seen:
                seen[x] += 1
                new.append( "%s_%d" % (x, seen[x]))
            else:
                seen[x] = 0
                new.append(x)
        return new

    data.columns = rename_duplicates(list(data.columns))

    for index, row in data.iterrows():
        for player in data.columns[:-2]:
            playerName = row[player].split("(")[0]
            names.append(playerName)
            positions.append(player)
            player = optimizer.get_player_by_name(playerName)
            fppgs.append(player.fppg)
            teams.append(player.team)
            salaries.append(player.salary)
        positions.append("Totals")
        names.append("")
        teams.append("")
        fppgs.append(row["FPPG"])
        salaries.append(row["Budget"])

        positions.append("")
        names.append("")
        teams.append("")
        fppgs.append("")
        salaries.append("")

        # print row["PG"]

    lineups["Positions"] = positions
    lineups["Name"] = names
    lineups["Team"] = teams
    lineups["Projection"] = fppgs
    lineups["salaries"] = salaries

    result = {"lineups": lineups.values.tolist(),
              "export": [list(data.columns)] + data.values.tolist()}
    
    del payload
    
    return jsonify(result)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET'])
def init():
    return redirect("/static/test.html")           
@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        sport_type = request.form.get("sport_type")
        slate_name = request.form.get("slate_name")
        upload_type = request.form.get("upload_type")
        site = request.form.get("site")
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = ""
            if sport_type == "PGA":
                filename = secure_filename(site + "_PGA.csv")
            if sport_type == "NFL":
                filename = secure_filename(site + "_NFL.csv")
            if sport_type == "NBA":
                filename = secure_filename(site + "_NBA.csv")
            
            if os.path.exists(UPLOAD_FOLDER + filename):
                os.remove(UPLOAD_FOLDER + filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            print ("file uploaded: " + UPLOAD_FOLDER + filename)
            filename = UPLOAD_FOLDER + filename
            csv_list = list()
            projection_list = list()
            if sport_type == "NFL":
                with open(filename, 'r') as file:
                    csv_reader = csv.reader(file)
                    i = 0
                    for row in csv_reader:
                        if i != 0:
                            if site == "FanDuel":
                                new_row = list()
                                new_row.append(row[0])
                                if row[3] == None or row[3] == '':
                                    new_row.append("0")
                                    break
                                else:
                                    if ("$" in row[3]):
                                        new_row.append(row[3][1:].replace(",",""))
                                    else:
                                        new_row.append(row[3])
                                if row[1] == 'DST':
                                    new_row.append('D')
                                else:
                                    new_row.append(row[1])
                                if row[2] == None or row[2] == '':
                                    new_row.append('No')
                                else:
                                    new_row.append(row[2])
                                new_row.append(row[6])
                                csv_list.append(new_row)
                            projection_row = list()
                            projection_row.append(row[0])
                            projection_row.append(row[4])
                            projection_list.append(projection_row)
                        i = i + 1
                cur = mysql.connection.cursor()
                if site == "DraftKings":
                    add_projection_query = """Update slates set projection = %s where site = %s and sport_type = %s"""
                    data_salary = (json.dumps(projection_list), 'DraftKings', 'NFL')
                    cur.execute(add_projection_query, data_salary)
                    mysql.connection.commit()
                    cur.close()
                else:
                    sql_delete_query = """delete from slates where site = %s and sport_type = %s"""
                    cur.execute(sql_delete_query, ("FanDuel", "NFL"))
                    mysql.connection.commit()
                    cur.close()
                    cur = mysql.connection.cursor()
                    add_salary_query = """INSERT INTO slates (site, sport_type, slate_name, salary, projection) VALUES (%s, %s, %s, %s, %s)"""
                    data_salary = ('FanDuel', sport_type, slate_name, json.dumps(csv_list), json.dumps(projection_list))
                    cur.execute(add_salary_query, data_salary)
                    mysql.connection.commit()
                    cur.close()
            if sport_type == "NBA":
                with open(filename, 'r') as file:
                    csv_reader = csv.reader(file)
                    i = 0
                    for row in csv_reader:
                        if i != 0:
                            projection_row = list()
                            projection_row.append(row[0])
                            projection_row.append(row[1])
                            projection_list.append(projection_row)
                        i = i + 1
                cur = mysql.connection.cursor()
                add_projection_query = """Update slates set projection = %s where site = %s and sport_type = %s"""
                    data_salary = (json.dumps(projection_list), site , 'NBA')
                    cur.execute(add_projection_query, data_salary)
                    mysql.connection.commit()
                    cur.close()
                if site == "FanDuel":
                   exec('scrape_scrape_NBA_FD.py')      
            if sport_type == "PGA":
                ['Name', 'Salary', 'Position', 'TeamAbbrev',  'ID']
                with open(filename, 'r') as file:
                    csv_reader = csv.reader(file)
                    for row in csv_reader:
                        if row[0] != 'Id':
                            new_row = list()
                            new_row.append(row[3])
                            new_row.append(row[7])
                            new_row.append(row[1])
                            new_row.append(row[9])
                            new_row.append(row[0])
                            csv_list.append(new_row)
                cur = mysql.connection.cursor()
                if upload_type == "New":
                    sql_delete_query = """delete from slates where site = %s and sport_type = %s"""
                    cur.execute(sql_delete_query, ("FanDuel", sport_type))
                    mysql.connection.commit()
                    cur.close()
                    cur = mysql.connection.cursor()
                sql_select_query = """select absid from slates where site = %s and sport_type = %s and slate_name = %s"""
                cur.execute(sql_select_query, ("FanDuel", sport_type, slate_name))
                records = cur.fetchone()
                count = cur.rowcount
                cur.close()
                cur = mysql.connection.cursor()
                add_salary_query = ""
                data_salary = ('FanDuel', sport_type, slate_name, json.dumps(csv_list))
                if count < 1:
                    add_salary_query = """INSERT INTO slates (site, sport_type, slate_name, salary) VALUES (%s, %s, %s, %s)"""
                else:
                    add_salary_query = """Update slates set salary = %s, slate_name = %s where absid = %s"""
                    data_salary = (json.dumps(csv_list), slate_name, records[0])
                cur.execute(add_salary_query, data_salary)
                mysql.connection.commit()
                cur.close()
            return redirect("/static/test.html")
    return '''
    <!doctype html>
    <title> Fantasy Sport Optimizer</title>
    <h1>Please select a file to upload.</h1>
    
    <form method=post enctype=multipart/form-data>
      <select name="sport_type">
        <option value="NFL">NFL</option>
        <option value="PGA">PGA</option>
        <option value="NBA">NBA</option>
      </select>
      <select name="site">
        <option value="DraftKings">DRAFTKINGS</option>
        <option value="FanDuel">FANDUEL</option>
      </select>
      <select name="upload_type">
        <option value="Add">ADD</option>
        <option value="New">NEW</option>
      </select>
      <input name="slate_name">
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''
@app.route('/uploadProjections', methods=['GET', 'POST'])
def upload_projections():
    print(request)
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        slate = request.form.get("slate")
        sport_type = request.form.get("sport_type")
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename("projections.csv")
            if os.path.exists(UPLOAD_FOLDER + filename):
                os.remove(UPLOAD_FOLDER + filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            print ("file uploaded: " + UPLOAD_FOLDER + filename)
            filename = UPLOAD_FOLDER + filename
            csv_list = list()
            # ['Name', 'Salary', 'Position', 'TeamAbbrev',  'ID']
            with open(filename, 'r') as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    if row[0] != 'Name':
                        new_row = list()
                        new_row.append(row[0])
                        if (sport_type == "PGA"):
                            new_row.append(row[13])
                        if (sport_type == "NFL"):
                            new_row.append(row[12])
                        csv_list.append(new_row)
            cur = mysql.connection.cursor()
            update_projections_query = """Update slates set projection = %s where absid = %s"""
            data_salary = (json.dumps(csv_list), slate)
            cur.execute(update_projections_query, data_salary)
            mysql.connection.commit()
            cur.close()
            return redirect("/static/test.html")
    return redirect("/static/test.html")
from flask import send_file

@app.route('/api/players', methods=['GET', 'POST'])
def api_players():

    gc.collect()
    # coreCols = ["Name", "Salary", "Position", "TeamAbbrev", "Projection", "Custom"]
    print ("player api called")
    if request.method == 'GET':
        slate = request.args.get('slate')
        cur = mysql.connection.cursor()
         
        sql_select_query = """select salary, projection from slates where absid = %s"""
        cur.execute(sql_select_query, (slate, ))
        row = cur.fetchone()
        lineup_data = list()
        data = list()
        result = list()
        temp_data = list()
        for player in json.loads(row[0]):
            new_row = list()
            lineup_row = list()
            new_row.append("0")     # Lock
            new_row.append("0")     # Remove
            new_row = new_row + player[0:4]

            lineup_row = lineup_row + player[0:4]
            i = 0
            if row[1]!=None:
                for projection in json.loads(row[1]):
                    if projection[0].replace(".","") in new_row[2]:
                        new_row.append(projection[1])
                        lineup_row.append(projection[1])
                        i = 1
                        break
            if i == 0:
                new_row.append(0)
                lineup_row.append(0)
            new_row.append(new_row[6])
            new_row.append(50)
            lineup_row.append(new_row[6])
            lineup_row.append(50)
            lineup_row.append(player[4])

            temp_row = list(new_row)
            temp_row.append(player[4])
            
            if new_row[3] != None and new_row[3] !='0':
                try:
                    new_row.insert(7, str(round(Decimal(new_row[6])/Decimal(new_row[3])*1000, 2)))
                except Exception:
                    new_row.insert(7,0)
            else:
                new_row.insert(7,0)
            data.append(new_row)
            lineup_data.append(lineup_row)
            temp_data.append(temp_row)
        cur.close()
        result.append(data)
        result.append(lineup_data)
        result.append(temp_data)
     
        del lineup_data
        del temp_data
        del data
        del new_row
        del temp_row
        del lineup_row

        return jsonify(result)
    if request.method == 'POST':
        coreCols = ["Name", "Salary", "Position", "TeamAbbrev", "Projection", "Custom"]
        # print ("post called", sys.stderr)
        data = json.loads(request.data.decode('utf-8'))

        lineup_data = data["lineup_data"]
        players = data["players"]
        lineups = data["lineups"]
        site = data["site"]
        projection = data["projection"]
        sport_type = data["sport_type"]
        data = pd.DataFrame.from_records(players)
        data.columns =  ["Lock", "Remove"] + coreCols + ["Exposure", "ID"]
        #filename = os.path.join(os.path.dirname(__file__),'lineup_temp.csv')
        lineup_writer = open('/home/johnh/lineup_temp.csv', 'w+')

        # create the csv writer object

        csvwriter = csv.writer(lineup_writer)

        header = ["Name", "Salary", "Position", "TeamAbbrev", "Projection", "Custom", "Exposure", "ID"]
        csvwriter.writerow(header)

        for r in lineup_data:
            csvwriter.writerow(r)
                        
        lineup_writer.close()
        raw = None
        raw = pd.read_csv("/home/johnh/lineup_temp.csv")
        
        payload = {'posted': data.to_json(),
                   "raw": raw.to_json(),
                   "site": site,
                   "sport_type": sport_type,
                   "projection": projection,
                   "lineups": lineups}

        del lineup_data
        del players
        del lineups
        del site
        del projection
        del sport_type

        return optimizeJSON(payload)

@app.route('/api/slates', methods=['GET', 'POST'])
def api_slates():
    print ("slate api called")
    if request.method == 'GET':
        site = request.args.get('site')
        sport_type = request.args.get('sport_type')
        cur = mysql.connection.cursor()
        slate_list = list()
        
        sql_select_query = """select absid, slate_name, slate_time from slates where site = %s and sport_type = %s"""
        
        # cur_date = "%" + datetime.today().strftime('%Y-%m-%d') + "%"
        cur.execute(sql_select_query, (site, sport_type, ))
        records = cur.fetchall()

        for row in records:
            slate_info = {}
            slate_info["id"] = row[0]
            if row[2] != None:
                slate_info["name"] = row[2][5:10] + " " + row[2][11:16] + " " + row[1]
            else:
                slate_info["name"] = row[1]
            slate_list.append(slate_info)
        cur.close()
        return jsonify(slate_list)