from flask import Flask

app = Flask(__name__)

HTML = '''
<style>
    h1, h2, img {
        display: block;
        max-width:80%;
        width: auto;
        height: auto;
        margin-left: 10%;
    }
</style>

<h1>Static Dashboard</h1>

<h2>Overview</h2>
<img src="https://petra.carrion.io/static_dashboard/imgs/overview.png" alt="Overview">

<h2>Top Five Cities</h2>
<img src="https://petra.carrion.io/static_dashboard/imgs/top_5_cities.png" alt="Top Five Cities">

<h2>Top Five Technologies</h2>
<img src="https://petra.carrion.io/static_dashboard/imgs/top_5_technologies.png" alt="Top Five Technologies">
'''


@app.route('/')
def index():
    return HTML

