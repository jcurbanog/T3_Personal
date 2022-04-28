from PyQt5 import QtWidgets, QtGui, QtCore, uic
from PyQt5.QtSql import QSqlQueryModel, QSqlDatabase, QSqlQuery
import os, glob, shutil, errno
import pandas as pd
import glob
import numpy as np
import sqlite3
import sys
from datetime import timedelta
def convertDates(df: pd.DataFrame):
    """
    Convert dates from a list of strings by testing several different input formats
    Try all date formats already encountered in data points
    If none of them is OK, try the generic way (None)
    If the generic way doesn't work, this method fails
    (in that case, you should add the new format to the list)
    
    This function works directly on the giving Pandas dataframe (in place)
    This function assumes that the first column of the given Pandas dataframe
    contains the dates as characters string type
    
    For datetime conversion performance, see:
    See https://stackoverflow.com/questions/40881876/python-pandas-convert-datetime-to-timestamp-effectively-through-dt-accessor
    """
    formats = ("%m/%d/%y %H:%M:%S", "%m/%d/%y %I:%M:%S %p",
               "%d/%m/%y %H:%M",    "%d/%m/%y %I:%M %p",
               "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %I:%M:%S %p", 
               "%d/%m/%Y %H:%M",    "%d/%m/%Y %I:%M %p",
               "%y/%m/%d %H:%M:%S", "%y/%m/%d %I:%M:%S %p", 
               "%y/%m/%d %H:%M",    "%y/%m/%d %I:%M %p",
               "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %I:%M:%S %p", 
               "%Y/%m/%d %H:%M",    "%Y/%m/%d %I:%M %p",
               "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %I:%M:%S %p",
               "%Y:%m:%d %H:%M:%S", "%Y:%m:%d %I:%M:%S %p",
               "%m:%d:%Y %H:%M:%S", "%m:%d:%Y %I:%M:%S %p",None)
    times = df[df.columns[0]]
    for f in formats:
        try:
            # Convert strings to datetime objects
            new_times = pd.to_datetime(times, format=f)
            # Convert datetime series to numpy array of integers (timestamps)
            new_ts = new_times.values.astype(np.int64)
            # If times are not ordered, this is not the appropriate format
            test = np.sort(new_ts) - new_ts
            if np.sum(abs(test)) != 0 :
                #print("Order is not the same")
                raise ValueError()
            # Else, the conversion is a success
            #print("Found format ", f)
            df[df.columns[0]] = new_times
            return
        
        except ValueError:
            #print("Format ", f, " not valid")
            continue
    
    # None of the known format are valid
    raise ValueError("Cannot convert dates: No known formats match your data!")

def processData(trawfile, prawfile):
        
    # On charge les données
    dftemp = pd.read_csv(trawfile, skiprows=1)
    dfpress = pd.read_csv(prawfile, skiprows=1)

    # On renomme (temporairement) les colonnes, on supprime les lignes sans valeur et on supprime l'index
    val_cols = ["Temp1", "Temp2", "Temp3", "Temp4"]
    all_cols = ["Idx", "Date"] + val_cols
    for i in range(len(all_cols)) :
        dftemp.columns.values[i] = all_cols[i] 
    dftemp.dropna(subset=val_cols,inplace=True)
    dftemp.dropna(axis=1,inplace=True) # Remove last columns
    dftemp.drop(["Idx"],axis=1,inplace=True) # Remove first column
    val_cols = ["tension_V", "Temp_Stream"]
    all_cols = ["Idx", "Date"] + val_cols
    for i in range(len(all_cols)) :
        dfpress.columns.values[i] = all_cols[i]
    dfpress.dropna(subset=val_cols,inplace=True)
    dfpress.dropna(axis=1,inplace=True) # Remove last columns
    dfpress.drop(["Idx"],axis=1,inplace=True) # Remove first column

    # On convertie les dates au format yy/mm/dd HH:mm:ss
    convertDates(dftemp)
    convertDates(dfpress)
    # On vérifie qu'on a le même deltaT pour les deux fichiers
    # La référence sera l'écart entre les deux premières lignes pour chaque fichier 
    # --> Demander à l'utilisateur de vérifier que c'est ok
    dftemp_t0 = dftemp.iloc[0,0]
    dfpress_t0 = dfpress.iloc[0,0]
    deltaTtemp = dftemp.iloc[1,0] - dftemp_t0
    deltaTpress = dfpress.iloc[1,0] - dfpress_t0
    if deltaTtemp != deltaTpress :
        pass
    else : 
        deltaT = deltaTtemp

    # On fait en sorte que les deux fichiers aient le même t0 et le même tf
    dftemp_tf = dftemp.iloc[-1,0]
    dfpress_tf = dfpress.iloc[-1,0]
    print(dfpress_t0)
    print(dftemp_t0)
    if dfpress_t0 < dftemp_t0 : 
        while dfpress_t0 != dftemp_t0:
            dfpress.drop(dfpress.head(1).index[0], inplace=True)
            dfpress_t0 = dfpress.iloc[dftemp.head(1).index[0],0]
    elif dfpress_t0 > dftemp_t0 : 
        while dfpress_t0 != dftemp_t0:
            dftemp.drop(dftemp.head(1).index[0], inplace=True)
            dftemp_t0 = dftemp.iloc[dftemp.head(1).index[0],0]

    if dfpress_tf > dftemp_tf:
        while dfpress_tf != dftemp_tf :
            dfpress.drop(dfpress.tail(1).index[0], inplace=True)
            dfpress_tf = dfpress.iloc[-1,0]
    elif dfpress_tf < dftemp_tf:
        while dfpress_tf != dftemp_tf :
            dftemp.drop(dftemp.tail(1).index[0], inplace=True)
            dftemp_tf = dftemp.iloc[-1,0]

    # On supprime les lignes qui ne respecteraient pas le deltaT
    i = 1
    while i<dftemp.shape[0]:
        if ( dftemp.iloc[i,0] - dftemp.iloc[i-1,0] ) % deltaT != timedelta(minutes=0) :
            dftemp.drop(dftemp.iloc[i].name,  inplace=True)
        else :
            i += 1
    i = 1
    while i<dfpress.shape[0]:
        if ( dfpress.iloc[i,0] - dfpress.iloc[i-1,0] ) % deltaT != timedelta(minutes=0) :
            dfpress.drop(dfpress.iloc[i].name,  inplace=True)
        else :
            i += 1

    return dftemp, dfpress

dft = "/home/jurbanog/T3/Molonari/Molonari_2021/resources/sampling_points/study_2022/Point051-2/point051-2_T_measures.csv"
dfp = "/home/jurbanog/T3/Molonari/Molonari_2021/resources/sampling_points/study_2022/Point051-2/point051-2_P_measures.csv"

dft, dfp = processData(dft,dfp)
print(dft)
print(dfp)