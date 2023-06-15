# rezAWARE

![Reservation Gateway](./img/rezgate_logo.png?raw=true "RezGateway") 

The README file is useful for projects that are using __rezaware__ platform for AI/ML and augmented BI pipelines. It is designed to integrate data wrangling, mining, and visualization in to a single coherent project. Here we introduce ways for getting started with the platform framework. The [WIKI](/wiki) for comprehensive documentation on the rezaware methodology, functional components, and behaviour.

__NOTE__: instructions and content is specific to Debian distros and was tested on Ubuntu 20.04.

Table of Content
* [Starting a New Project](#starting-a-new-project)
* Re-configuring an existing Project
* 

## Starting a New Project
1. Create an empty git repository with the a desired project name; e.g., __MyNewProj__ . 
   * Presupose that you have [git installed and initialized](https://phoenixnap.com/kb/how-to-install-git-on-ubuntu) on your computer.
   * You may also consider [setting up an Anaconda environment](https://salishsea-meopar-docs.readthedocs.io/en/latest/work_env/python3_conda_environment.html) with __python-3.8.10__; 
      - e.g., ```conda create -n rezenv python=3.8.10```
   * Activate your conda environment;
      - e.g. ```conda activate rezenv```
3. Clone your _MyNewProj_ into a desired director location; for example
   * ```cd ~/all_rez_projects/```
   * ```git clone https://github.com/<my_git_user_name>/MyNewProj.git```
4. Move into the newly created project folder
   * ```cd ~/all_rez_projects/MyNewProj```
5. Now clone and initialize rezaware platform as a [submodule](https://github.blog/2016-02-01-working-with-submodules/)
   * ```git submodule add -b main https://github.com/waidyanatha/rezaware.git rezaware```
   * ```git submodule init```; will copy the mapping from the .gitmodules file into the local _./.git/config_ file
6. Navigate into the rezaware folder and run setup to initialize the project with AI/ML functional app classes
   * ```cd rezaware```
   * In the next command ```python3 -m 000_setup --with_ini_files```, 
      - it is important to use the _--with_ini_files_ directive flag. 
      - This instructs _000_setup.py_ to build the _wrangler_, _mining_, and visuals folders structure
      - and the respective python __init.py__ and __app.ini__ files necessary for the seamless package integration
      - execute ```python3 -m 000_setup --with_ini_files```
   * You have now created your _MyNewProj_ with the _rezaware_ platform framework.
1. Change back to the project director
   * ```cd ..``` or ```cd ~/all_rez_projects/MyNewProj```
1. Add the submodule and initialize
   * ```git add .gitmodules rezaware/```
   * ```git init```
1. (Optional) Include a _README.md_ file, if not already
   * ```echo "# Welcome to MyNewProj" >> README.md```
1. Add and commit all newly created files and folders in _MyNewProj_
   * ```git add .```
   * ```git commit -m "added rezaware submudle and setup project"```
1. Push the submodule and new commits to the repo
   * ```git push origin main```
   * Check your github project in the browser; you will see a folder ___rezaware @ xxxxxxx___; where xxxxxxx is the last 7 digits from the _rezaware.git_ repo commit code 

## Introduction

The rezAWARE tools and processes will support an organization to mature from descriptive to cognitive analytics. For such data being the key ingrediant, the rezAWARE core components offer:
1. _Mining_ - Arificial Intelligence (AI) and Machine Learning (ML) analytical methods
1. _Utils_ - set of common utility packages that can be used with any of the below apps
1. _Wrangler_ - for processing data extract, transform, and load automated pipelines
1. _Visuals_ - interactive dashboards with visual analytics for Business Intelligence (BI)

## Quickstart
__Linux Only__
* Simply download and run the setup file
   * ```python3 setup.py```

#### Setup will guide you with
* creating a new user and home directory
* starting a new conda environment
* cloning rezaware pltform
* installing all dependencies
* starting all databases and storage tools
* configuring for your environment

#### Test the new setup
Run __pytest__ by executing the command in your terminal prompt
* ```pytest```


## Developers

### Prerequisits

#### Clone rezaware
* [Install Git on you machine](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* Create a fork of the rezaware platform
   * ```http://github/myrepo/rezaware```
* Make a working director on you machine; e.g., in linux prompt:
   * ```mkdir ~/workspace/```
* Navigate to that director; e.g., in linux prompt: 
   * ```cd ~/workspace/```
* Initialize your git in this directory 
   * ```git init```
* Clone the rezaware platform in to you local machine,
   * ```git clone https://github.com/myrepo/rezaware.git
* Change your directory to the rezaware folder; e.g., 
   * ```cd ~/workspace/rezaware```

#### Conda environment
* It is recommended to setup a clean [Anaconda3](https://www.anaconda.com/) environment with Python 3.8.10 to avoid any distutils issues. 
* After you have installed __conda__; create a new environment using the _requirements.txt_ file:
   * ```conda create --name rezenv --file requirements.txt```
* Thereafter, check if all packages, listed in requirements.txt was installed
   * ```conda list``` will print a list to stdout like this

    ```
    
    # packages in environment at /home/nuwan/anaconda3/envs/reza:
    #
    # Name                    Version                   Build  Channel
    _libgcc_mutex             0.1                        main  
    _openmp_mutex             5.1                       1_gnu  
    absl-py                   1.2.0                    pypi_0    pypi
    alembic                   1.8.1                    pypi_0    pypi
    amqp                      5.1.1                    pypi_0    pypi
    aniso8601                 9.0.1                    pypi_0    pypi
    anyio                     3.6.1                    pypi_0    pypi
    apache-airflow            2.3.4                    pypi_0    pypi
    apache-airflow-providers-celery 3.0.0              pypi_0    pypi
    ...
    
    ```

___Please note that the getting started steps have not been tested and should be revised with missing steps.___

