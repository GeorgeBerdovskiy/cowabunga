# -165a-winter-2024

## Setup

### Venv
Venv is Python's virtual environment. Python now mandates that packages either be installed globally by a package manager (not pip) or installed to a virtual environment with pip or some pip replacement. Venv is best suited for this use case, so to set up a virtual environment, you can use the following commands.

```
python3 -m venv venv
```

Alternatively, if you want your `venv/` folder to not clutter up the directory, you can call `venv/` `./venv/` like the following example.

```
python3 -m venv .venv
```

You can then activate the environment by running

```
source venv/bin activate

# or
source .venv/bin activate
```

This is a bit different on Windows.

More info [here](https://docs.python.org/3/library/venv.html)

### Installing Rust

1. Go to [`rustup.rs`](https://rustup.rs/) to install rust.

2. Setup rust

```sh
rustup override set stable
rustup update stable
```
 
## Running Rust!

```
cargo build

pip install .

python3 __main__.py
```

```
Hello from Rust!
[INFO] [logger.py] Logger initialized!
[INFO] [db.py] Creating table "Grades" with 5 columns and key index 0...
[INFO] [db.py] Returning new table "Grades"
Inserting 10k records took:  			 0.0014072789999999974
Updating 10k records took:  			 0.0062958879999999995
Selecting 10k records took:  			 0.0027192699999999993
Aggregate 10k of 100 record batch took:	 3.9273999999998865e-05
Deleting 10k records took:  			 0.0005510730000000026
```
