from flask import Flask, session, g, redirect, url_for, escape, request, render_template
from time import gmtime, strftime, localtime
import sqlite3
import sys

app = Flask(__name__)

#######################
#   DATABASE CONFIG   #
#######################


@app.route('/', methods=['GET', 'POST'])
def index():
    return redirect(url_for('login'))
    # return render_template('search.html')
	
@app.route('/login', methods=['GET', 'POST'])
def login():
    # return render_template('search.html')
    if request.method == 'POST':
        username_form = request.form['uid']
        password_form = request.form['pwd']
        if username_form == 'ysgao':
            return redirect(url_for('search'))
            return render_template('search.html')
        else:
        	return render_template('index.html')
    return render_template('index.html')


@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        g.db = sqlite3.connect('scrapers.db')
        keyword = request.form['keyword']
        ein = request.form['ein']
        state = request.form['states']
        date = request.form['date']
        sortby = request.form['sortby']

        conditions = []

        flag = False

        if keyword:
            conditions.append("C_FName1 LIKE '%" + keyword + "%'")
            flag = True
        if ein:
            conditions.append("EIN = " + ein)
            flag = True
        if state:
            conditions.append("C_FSt = '" + state.upper() + "'")
            flag = True
        if date:
            conditions.append("TaxYr = " + date)
            flag = True
        conditionSQL = " AND ".join(conditions)
        if sortby:
            if sortby == 'Keyword':
                conditionSQL += " ORDER BY C_FName1"
            elif sortby == 'EIN':
                conditionSQL += " ORDER BY EIN"

        if flag is True:
            strSQL = "SELECT EIN, TaxYr, C_FName1, C_FAdd1, C_FCityNm, C_FSt, C_Fzip, E_Phone, EZ_I_Web FROM Main WHERE " \
                + conditionSQL
        else:
            strSQL = "SELECT EIN, TaxYr, C_FName1, C_FAdd1, C_FCityNm, C_FSt, C_Fzip, E_Phone, EZ_I_Web FROM Main " \
                + conditionSQL


        cur = g.db.execute(strSQL)
        results = [dict(EIN=row[0], TaxYr=row[1], C_FName1=row[2], C_FAdd1=row[3], C_FCityNm=row[4], C_FSt=row[5],
                        C_Fzip=row[6], E_Phone=row[7], EZ_I_Web=row[8]) for row in cur.fetchall()]

        return render_template('results.html', results=results)
    else:
        return render_template('search.html')


@app.route('/download', methods=['GET', 'POST'])
def download():
    if request.method == 'POST':
        g.db = sqlite3.connect('task.db')
        title = request.form['title']
        priority = request.form['priority']
        time = strftime("%Y-%m-%d %H:%M:%s", localtime())
        sql = "insert into tasks (title, priority, date) values ('" + title + "','" + priority + "','" + time + "');"
        g.db.execute(sql)
        g.db.commit()
        cur = g.db.execute('select title, priority, date from tasks order by id desc')
        results = [dict(title=row[0], priority=row[1], date=row[2]) for row in cur.fetchall()]                              
        return render_template('download_results.html', results=results)
    else:
        return render_template('download.html')
   
# @app.route('/search')
# error = None
    



if __name__ == '__main__':
    app.run(debug=True)