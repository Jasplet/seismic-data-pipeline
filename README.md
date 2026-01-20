# seismic-data-pipeline
Code to download miniseed data from Certimus/Minimus type seismometers. 
Initially developed for a project in Oxford, but has now hopefully been generalised for wider use. 

## Installation

### Using uv (recommended)

```bash
# Clone the repository
git clone https://github.com/kendall-group-oxford/seismic-data-pipeline.git
cd seismic-data-pipeline

# Create and activate virtual environment
uv venv
source .venv/bin/activate  # Linux/Mac
# OR
.venv\Scripts\activate     # Windows

# Install package
uv pip install -e .

# Install with development dependencies
uv pip install -e ".[dev]"
```

### Using pip + venv

```bash
# Clone the repository
git clone https://github.com/yourusername/seismic-data-pipeline.git
cd seismic-data-pipeline

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# OR
.venv\Scripts\activate     # Windows

# Install package
pip install -e .

# Install with development dependencies
pip install -e ".[dev]"
```

## Examples

Example use-case scripts can be found in `example_scripts`. These scripts are generalised and you will need to replace the dummy varialbes with your own, meaningful, ones.

Example scripts:
 - `bulk_data_download.py`
 - `daily_remote_download.py`
 - `date_range_download.py`
 - `run_from_configfile.py`
 - `find_data_gaps.py`
 - `make_seed_compliant_names.py`

## Output file naming

This code uses the following filenaming convention under the top level provided `<data_dir>`:

```<data_dir>/<year>/<month>/<day>/<network_code>.<station_code>.<location_code>.<channel>.<year><month><day>T<hour><minute><second>.mseed```

For example, data requested from a station the netowrk code `OX`, station code `STA1`, location code `00`, and channel code `HHZ` for `2026/01/01` starting at `12:00:00` would be written to 

```<data_dir>/2026/01/01/OX.STA1.00.HHZ.20260101T120000.mseed```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Write tests for new functionality
4. Ensure all tests pass (`pytest`)
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

**J. Asplet**  
Department of Earth Sciences, University of Oxford  
Contact: Joseph.Asplet@earth.ox.ac.uk 

## Citation

If you use this software in your research, please consider citing:

```bibtex
@software{asplet2024seismic,
  author = {Asplet, J.},
  title = {seismic-data-pipeline: Asynchronous miniSEED data retrieval},
  year = {2024},
  url = {https://github.com/kendall-group-oxford/seismic-data-pipeline}
}
```

## Acknowledgments

Developed as part of seismic monitoring research at the University of Oxford.

## Support

For bugs and feature requests, please [open an issue](https://github.com/kendall-group-oxford/seismic-data-pipeline/issues).


## To-Dos

2. Improve packaging / gathering of files.
3. Fold in EIDA/FDSN requests.
4. Async requests for regular downloads
    a. Reorgansie gapfill script
    b. add tests for async functions
    c. make daily download use async functions
