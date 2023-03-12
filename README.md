# myFin Data

This project is collect daily stock price data and saved in MySQL. It donwnloads data from Yahoo Finance.

### 1) Create a virtualenv named myFinData  
   ```
   mkdir ~/env/myFinData  
   virtualenv myFinData -p 3.8  
   source ~/env/myFinData/bin/activate  
   cd to myFinData local directory  
   pip install -r requirements.txt  
   ```

### 2) Create a docker image to run the daily batch job to predict stock price 
   ```
   docker build --no-cache -f Dockerfile-dev9.1.tf2-py3 -t spred-dev9.1:tf2-py3 .
   ``` 
