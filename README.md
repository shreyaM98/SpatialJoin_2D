# Processing Spatial Join on 2D data

This project can be executed in Intellij by creating a Maven project and adding Hadoop dependency in the pom.xml file.<br />
This project uses Hadoop version 3.2.0 <br />

 CreateDatasets creates two large datasets Point and Reactangle (>100 MB) using a random function. All the coordinates of a rectangle integer values. <br />

 Spatial2dJoin.java contains the job configuration and the main method. <br />

 There are separate classes implemented for Mappers and Reducers. <br />

 Program takes 3 inputs - path to points dataset, points to rectangle dataset, spatial window coordinates (x1,y1,x2,y2); <br /> 

 Final Output will be key-value pairs: <r1, (3,15)> <r2, (2,4)> ...
