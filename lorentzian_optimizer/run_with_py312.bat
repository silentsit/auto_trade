1   @echo off
   echo Using Python 3.12...
   py -3.12 -m venv venv
   call venv\Scripts\activate.bat
   py -3.12 -m pip install -r requirements.txt
   py -3.12 main.py
   pause