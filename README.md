# Optimal Data Engine (ODE) Metrics Vault Build 20161012 #
Copyright 2016 OptimalBI - Licensed under the GNU GENERAL PUBLIC LICENSE Version 3.0 (GPL-3.0)

## The Approach: ##
Good architecture is modular, separating concerns across its components. This helps ensure each component does one job well, which in turn helps the components work together, delivering a valuable experience to users. Modular architecture is also easier to maintain.
A useful data warehouse does not attempt to take data directly from source systems to end-users in one hit, instead it uses modular components we call data layers. While these modular components have many specific names (e.g. “staging”, “foundation”, “presentation”) what ties them together is their layered nature: each one does a single job well, before passing data on to another layer.
Many of the ideological battles of the past (e.g. Inmon vs Kimball) were founded on an assumption that one methodology must rule them all. As soon as we think about a data warehouse in terms of layers, we are free to choose the optimal methodology for the job each layer is doing.

## What Optimal Data Engine (ODE) Metrics Vault does: ##
Using the Data Vault methodology, ODE breaks each complex part of your data processing into layers to make it easier and faster to manage and implement. It's a true agile methodology for building a Data Warehouse. ODE breaks the complexity into layers - to find out more visit http://www.ode.ninja/data-layers/

## Requirements (All users): ##
To Install ODE metrics Vault, you need to have:
* Access to a running Microsoft SQL Server (for example, Express Edition)
* SQL Server Management Studio installed on a computer which you can access
* Administrative rights to be able to create databases
* ODE installed (follow the installation instructions from https://github.com/OptimalBI/optimal-data-engine-mssql)

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

* Open SQL Server Management studio and load *MetricsVaultConfigRelease.sql* from the extracted zip file. You will find it in the *Config_release* folder.

* Within the script optionally change the DatabaseName and DefaultFilePrefix in the code to the preferred Configuration database name; default is *ode_to_mssql*. *ODE_Config* is recommended. 
* Click Execute from the toolbar. This should run successfully with a result of 'Update complete' on the Message panel 
* The Results panel of Management Studio query execution window should show 13 rows, which are the contents of the log4.JournalControl table.

## Current functionality: ##
Details of the current functionality can be found here http://www.ode.ninja/category/features/

## Notes ##
* Untested on SQL Server editions prior to 2014
* This product is still in Beta and should not be deployed to a production environment without thorough testing by you to ensure no adverse effects on your environment

## Feedback, suggestions, bugs, contributions: ##
Please submit these to GitHub issue tracking or join us in developing by forking the project and then making a pull request!

## Find out more: ##
Visit http://www.ode.ninja/ - this is where we keep our guides and share our knowledge. To find out more about OptimalBI and the work we do visit http://www.optimalbi.com or check out our blogs at http://optimalbi.com/blog/tag/data-vault/ for all the latest on our Data Vault journey. If you want to get in touch, you can email us at hey@optimalbi.com

## Change log: ##
```
Build 002.002.001 on 20160819
        * Added the ability to pass a parameter from the scheduler, to a Staging Procedure, telling the Procedure what type of run to perform (Full or DELTA).
```