# Extreme-Weather-Analysis

CSE 603(Parallel and Distributed Processing) - This project analyses  weather data and makes predictions of extreme weather using machine learing algorithm SVM(Support Vector Machines). It uses SPARK for pre-processing the data set and generated features and labels which will be used as input for the ML algorithm. The algorithm generates a model and this model can be used to make predictions and calculate accuracy of the predictions.

Usage for Python: 
1)spark-submit spark_script.py local (To run it on a local machine)
2)sbatch slurm.sh (For running it on CCR (Center for Computational Research))

Usage for Scala:
1) Generate JAR file using scala script and sbt.
2)spark-submit --class "ExtremeWeather" --master local [location of JAR] (To run it on local Machine)
3)sbatch scala_slurm.sh (For running it on CCR)

