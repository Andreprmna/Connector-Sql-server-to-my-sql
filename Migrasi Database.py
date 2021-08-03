import pandas as pd
from pandas.core.frame import DataFrame
from pandas.io import sql
import pyodbc
from sqlalchemy import create_engine
import pymysql
###################################################################################

server = 'DESKTOP-14K48F9'  # Masukkan Nama Server Anda
database = 'TestDB'  # nama database di sql server
username = 'sa'  # id user sql server
password = '1'  # password user sql server
koneksi = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server +
                         ';DATABASE='+database+';UID='+username+';PWD=' + password)
cursor = koneksi.cursor()
tableName = ['lainnya', 'Ref_Alasan', 'Ref_Bidang', 'Ref_Desa', 'Ref_Hak', 'Ref_Kab_Kota', 'Ref_Kap_Umur', 'Ref_Kecamatan',
             'Ref_Kondisi', 'Ref_Map5_17_108', 'Ref_Map_Rekening', 'Ref_Map_Unit', 'Ref_Masalah', 'Ref_Menu', 'Ref_Metode', 'Ref_Pemda', 'Ref_Pemilik',
             'Ref_Penyusutan', 'Ref_Pindahtangan', 'Ref_Provinsi', 'Ref_Rek0_108', 'Ref_Rek1_108', 'Ref_Rek2_108', 'Ref_Rek2_108', 'Ref_Rek3_108',
             'Ref_Rek4_108', 'Ref_Rek5_108', 'Ref_Rek_1', 'Ref_Rek_2', 'Ref_Rek_3', 'Ref_Rek_4', 'Ref_Rek_5', 'Ref_Riwayat', 'Ref_S_Bidang', 'Ref_S_Sub_Unit',
             'Ref_S_Unit', 'Ref_Setup', 'Ref_Sub_Unit', 'Ref_Tingkat', 'Ref_Unit', 'Ref_UPB', 'Ref_Version', 'RekAset_2_RincianObyek', 'Sheet3', 'Ta_Akses_Data',
             'Ta_Akses_Data_Rinc', 'Ta_Dev', 'Ta_Fn_KIB_A', 'Ta_Fn_KIB_B', 'Ta_Fn_KIB_C', 'Ta_Fn_KIB_D', 'Ta_Fn_KIB_E', 'Ta_Fn_KIB_F', 'Ta_Fn_KIB_L', 'Ta_FotoA', 'Ta_FotoB',
             'Ta_FotoC', 'Ta_FotoD', 'Ta_FotoE', 'Ta_KA', 'Ta_KA2', 'Ta_Kegiatan', 'Ta_KIB_A', 'Ta_KIB_B', 'Ta_KIB_C', 'Ta_KIB_D', 'Ta_KIB_E', 'Ta_KIB_F', 'Ta_KIB_Hps', 'Ta_KIBAHapus',
             'Ta_KIBAR', 'Ta_KIBBHapus', 'Ta_KIBBR', 'Ta_KIBCHapus', 'Ta_KIBCR', 'Ta_KIBDHapus', 'Ta_KIBDR', 'Ta_KIBEHapus', 'Ta_KIBER', 'Ta_KILER', 'Ta_KILHapus',
             'Ta_Kontrak_Addendum', 'Ta_Lainnya', 'Ta_LainnyaHapus', 'Ta_Manfaat', 'Ta_P3D', 'Ta_P3D_Rinc', 'Ta_Pemda', 'Ta_Pemeliharaan', 'Ta_Pemeliharaan_Rinc', 'Ta_PenA', 'Ta_PenB', 'Ta_PenC',
             'Ta_PenD', 'Ta_PenE', 'Ta_Pengadaan', 'Ta_Pengadaan_Bast', 'Ta_Pengadaan_Bast_Rinc', 'Ta_Pengadaan_Rinc', 'Ta_Pengadaan_SP2D', 'Ta_Pengadaan_SP2D_Rinc',
             'Ta_Penggunaan', 'Ta_Penghapusan', 'Ta_Penghapusan_Rinc', 'Ta_Pindahtangan', 'Ta_Program', 'Ta_RKBU', 'Ta_RKPBU', 'Ta_Ruang', 'Ta_SIPPT', 'Ta_Sub_Unit',
             'Ta_Susut_Ekspor', 'Ta_SusutB', 'Ta_SusutBL', 'Ta_SusutC', 'Ta_SusutCL', 'Ta_SusutD', 'Ta_SusutDL', 'Ta_SusutE', 'Ta_SusutEL', 'Ta_SusutL', 'Ta_UPB', 'Ta_User_G',
             'Ta_User_G_Menu', 'Ta_User_Satker', 'Ta_UserID', 'UPB_BMD_2_UPT_KEU', 'Ref_Rek_Aset1', 'Ref_Rek_Aset2', 'Ref_Rek_Aset3', 'Ref_Rek_Aset4', 'Ref_Rek_Aset5']
######################################## MAIN PROGRAM #############################
for i in range(len(tableName)):
    query = ("SELECT * FROM TestDB.dbo." + tableName[i])
    sql_query = pd.read_sql_query(query, koneksi)

    sqlEngine = create_engine(
        'mysql+pymysql://root:@127.0.0.1/bmd_aset', pool_recycle=3600)  # ganti /bmd_aset sesuai nama database yang dibuat di mysql
    dbConnection = sqlEngine.connect()

    try:
        frame = sql_query.to_sql(
            tableName[i], dbConnection, if_exists='fail')
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("Database Table Berhasil dibuat", tableName[i])
    finally:
        dbConnection.close()
print('Migrasi Selesai!!')
