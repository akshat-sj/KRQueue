from setuptools import setup, find_packages

setup(
    name='krq',
    version='0.1.0',
    description='KRQueue distributed task queue library',
    author='Akshat Singh Jaswal',
    author_email='sja.akshat@gmail.com',
    packages=find_packages(),
    install_requires=[
        'kafka-python-ng',
        'redis',
    ],
    scripts=[
        'examples/client.py',
        'examples/worker.py',
        'examples/worker2.py',
        'examples/client2.py',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.11',
)
