# main.py
from board.MainApplication import create_app

app = create_app()
# BEFORE RUNNING THE APP, MAKE SURE TO PLACE crime_sf.csv IN THE data FOLDER
if __name__ == '__main__':
    app.run(debug=True, port=8050)
