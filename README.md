# printobs
print offshore obs to screen

### Contact
Patrik Bohlinger, Norwegian Meteorological Institute, patrikb@met.no

## Purpose
Small program to print offshore obs to screen. All observations are being read from FROST API and need to be available there.

## Installation

1. Clone repository
```
git clone https://github.com/bohlinger/printobs.git
```

2. Install conda environment
```
cd printobs
conda env create -f environment.yml
```

3. Activate printobs
```
conda activate printobs
```

4. Create a .env file in printobs root directory and include your FROST client id
```
(printobs) user@pc:~/printobs$ cat .env
CLIENT_ID=
```

The FROST client_id can be obtained [here](http://louiseo.pages.obs.met.no/frost_workshop/tutorial/tutorial1/).

## Usage
Usage example and help can be obtained by typing

```
printobs.py -h
```
