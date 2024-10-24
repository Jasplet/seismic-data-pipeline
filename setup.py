from setuptools import setup, find_packages

setup(
    name="nymar_data_pipeline",         # Name of the project (should be unique)
    version="0.1.0",                  # Version of your project
    description="Data pipelining for remote Certimus deployments",
    author="Joseph Asplet",
    author_email="joseph.asplet@earth.ox.ac.uk",
    url="https://github.com/Jasplet/nymar-data-pipeline",  # URL to the project's repository (if available)
    
    # Automatically find all packages (folders with __init__.py) in your project
    packages=find_packages(),  
    
    # Include additional files listed in MANIFEST.in
    include_package_data=True,  
    
    # Project dependencies (install these when the package is installed)
    install_requires=[
        "numpy>=1.26.4",          # Example of a required package
        "obspy>=1.4.1",    # Specify version ranges, e.g., pandas 1.0 or higher
        # Add other dependencies your project needs
    ],

    # Classifiers for metadata, useful for PyPI (optional, but recommended)
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],

    # Minimum Python version requirement (optional)
    python_requires='>=3.11',
)