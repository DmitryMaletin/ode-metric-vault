# Optimal Data Engine (ODE) Metrics Vault Build 5.1 #
Copyright 2016 OptimalBI - Licensed under the GNU GENERAL PUBLIC LICENSE Version 3.0 (GPL-3.0)

## The Approach: ##
Collecting the data about the data is essentual in prooving that your Data Vault built with ODE is reliable and consistent. 

## What Optimal Data Engine (ODE) Metrics Vault does: ##
* Config audit
* Collecting the data about schedules execution
* Collecting the data stats
* Built-in data dictionary capability
More details: http://www.ode.ninja/ 

## Requirements (All users): ##
To Install ODE metrics Vault, you need to have:
* Access to a running Microsoft SQL Server (for example, Express Edition)
* SQL Server Management Studio installed on a computer which you can access
* Administrative rights to be able to create databases
* The latest version of ODE is installed (follow the installation instructions from https://github.com/OptimalBI/optimal-data-engine-mssql)

If you wish to develop ODE Metrics Vault further, we recommend:
* Visual Studio 2015 Community Edition installed on a computer which you can access. This can be downloaded from https://www.visualstudio.com/en-us/mt171547.aspx

## Branches: ##
Currently, ODE Metrics Vault has two Branches available:
* master and
* develop

Master contains code which is known to be running in a production environment.

Develop contains the latest, locally tested version of the codebase.

## Download and build instructions: ##
If you wish to develop the code base further, we recommend that you use the Visual Studio solution which is provided.

If you simply wish to build an ODE Metrics Vault instance and use it, the following instructions will direct you:

### Pre-requisites ###

* Latest version of ODE is installed
* A copy of ODE Metrics Vault project is copied to a temporary folder
 
### Installation ###

* Open SQL Server Management studio and load *Metrics_Stage_Release_Create.sql* from the extracted zip file. You will find it in the *Release_scripts* folder.
* Within the script optionally change the ODE_Config in the code to actual ODE Configuration database name; default is *ODE_Config*.
* Set query to SQLCMD mode.
* Click Execute from the toolbar. This should run successfully with a result of 'Update complete' on the Message panel 
* You can execute another script from this folder *ODEMetricsVault_ReportScripts.sql*. It creates a number of views which could be used for reporting on ODE Metrics Vault.

## Current functionality: ##
Details of the current functionality can be found here http://www.ode.ninja/

## Notes ##
* Untested on SQL Server editions prior to 2014
* This product is still in Beta and should not be deployed to a production environment without thorough testing by you to ensure no adverse effects on your environment

## Feedback, suggestions, bugs, contributions: ##
Please submit these to GitHub issue tracking or join us in developing by forking the project and then making a pull request!

## Find out more: ##
Visit http://www.ode.ninja/ - this is where we keep our guides and share our knowledge. To find out more about OptimalBI and the work we do visit http://www.optimalbi.com or check out our blogs at http://optimalbi.com/blog/tag/data-vault/ for all the latest on our Data Vault journey. If you want to get in touch, you can email us at hey@optimalbi.com

## Change log: ##
```
Build 005.001.001 on 20171011
		* Updated to match ODE version 5
Build 004.001.001 on 20170301
        * Updated to match ODE version 4
Build 002.002.001 on 20161103
        * Initial build with basic ODE Metrics Vault functionality

```
