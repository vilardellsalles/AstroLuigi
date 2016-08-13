from setuptools import setup, find_packages

setup(
      name="astroluigi",
      version="0.0.1",
      description="A tool for the creation of astronomical data reduction pipelines",
      url="??",
      author="Francesc Vilardell",
      author_email="vilardellsalles@gmail.com",
      license="BSD",
      classifiers=[
                   "Development Status :: 3 - Alpha",
                   "Intended Audience :: Developers",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: BSD License",
                   "Programming Language :: Python :: 3 :: Only"
                   "Topic :: Scientific/Engineering :: Astronomy"
                  ],
      packages=find_packages(),
      install_requires=["ccdproc>=1.0", "luigi>=2.0"]
     )
