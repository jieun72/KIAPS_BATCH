import argparse

from modules.amsua import amsua
from modules.sonde import sonde_innqc, sonde_grqc, sonde_ai
from modules.surface import surface_grqc, surface_innqc, surface_ai


class Main:
    def __init__(self, file_type):
        print("file_type: " + file_type)
        if file_type == "amsua_grqc" :
            amsua.Amsua().amsua_grqc("amsua_grqc")
        if file_type == "amsua_innqc" :
            amsua.Amsua().amsua_innoqc("amsua_innqc")
        
        if file_type == "sonde_grqc" :
            sonde_grqc.SondeGRQC().db_transfer("sonde_grqc")
        if file_type == "sonde_innqc" :
            sonde_innqc.SondeInnQC().db_transfer("sonde_innqc")
        if file_type == "sonde_ai" :
            sonde_ai.SondeAI().db_transfer("sonde_ai")
        
        if file_type == "surface_grqc" :
            surface_grqc.SurfaceGRQC().db_transfer("surface_grqc")
        if file_type == "surface_innqc" :
            surface_innqc.SurfaceInnQC().db_transfer("surface_innqc")
        if file_type == "surface_ai" :
            surface_ai.SurfaceAI().db_transfer("sonde_ai")
        
if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--date_time", default="2020061600")
        parser.add_argument("--file_type", default="amsua")
        args = parser.parse_args()
        Main(args.file_type)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(">>> Something went wrong: " + str(e))