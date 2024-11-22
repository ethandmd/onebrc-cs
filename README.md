## Setup
### Generate some weather stations measurements in Python.
`git clone https://github.com/gunnarmorling/1brc.git`
`cd 1brc\src\main\python`
`python3 create_measurements.py 1_000_000_000`

Note: there are other generators, pick your flavor. I don't like dealing with the JVM.

### Run
I setup a working director like so and hardcode the relative path to the `measurements.txt` at `../measurements.txt`: 
```
.
├── measurements.txt
├── onebrc-cs
│   ├── Program.cs
│   ├── bin
│   └── onebrc-cs.csproj
├── onebrc-haskell
├── onebrc-ocaml
└── onebrc-rs
```
Then run the program:
`dotnet run -c Release ./onebrc-cs`
