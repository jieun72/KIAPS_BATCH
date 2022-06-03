from modules.amsua import amsua
from modules.sonde import sonde, sonde_innqc, sonde_grqc, sonde_thin, sonde_xiv
from modules.surface import surface, surface_grqc, surface_innqc, surface_thin, surface_xiv


class Main:
    def __init__(self):
        # amsua.Amsua().amsua("amsua")
        # amsua.Amsua().amsua_grqc("amsua_grqc")
        # amsua.Amsua().amsua_xiv_before_thin("amsua_xiv_before_thin")
        # amsua.Amsua().amsua_xiv_ch("amsua_xiv_ch")

        # sonde.Sonde().db_transfer("sonde")
        # sonde_grqc.SondeGRQC().db_transfer("sonde_qrqc")
        # sonde_innqc.SondeInnQC().db_transfer("sonde_innqc")
        # sonde_thin.SondeThin().db_transfer("sonde_thin")
        # sonde_xiv.SondeXiv().db_transfer("sonde_xiv")

        surface.Surface().db_transfer("surface")
        surface_grqc.SurfaceGRQC().db_transfer("surface_grqc")
        surface_innqc.SurfaceInnQC().db_transfer("surface_innqc")
        surface_thin.SurfaceThin().db_transfer("surface_thin")
        surface_xiv.SurfaceXiv().db_transfer("surface_xiv")


if __name__ == '__main__':
    try:
        Main()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(">>> Something went wrong: " + str(e))
