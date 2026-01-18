

Analysis of the Adaptive Edge-Cloud IoT System
and Recommended Improvements
Project Overview and Goals
This project tackles the “Efficiency–Safety Paradox” in critical infrastructure monitoring (specifically water
pipelines). Traditional approaches either send all data to the cloud for safety (catch every anomaly, but at
massive bandwidth cost) or filter data at the edge for efficiency (save bandwidth, but risk missing high-
frequency events). Your thesis proposes a hybrid solution: a  hierarchical edge–cloud system with a
feedback loop that adapts sampling rate on demand. In normal operation, the edge sends aggregated
low-rate data (1 Hz) to minimize usage, but if a leak is detected or if extra capacity is available, the system
instantly switches to high-rate 50 Hz streaming – capturing detail only when needed. The expected benefits
are:    (1) Efficiency – ~98% bandwidth reduction vs baseline, (2) Responsiveness – sub-second leak detection
like a full-frequency system, and (3) Resilience – ability to prioritize critical data (leaks) even under cloud
load   or   network   constraints.   This   isn’t   just   about   building   a   water   monitor,   but   demonstrating   an
architecture where “fast reflexes” happen at the edge and the “brain” in the cloud coordinates globally. The
following analysis reviews your implementation details, compares them with state-of-the-art research
(including Dr. Atakan Aral’s work), and suggests improvements in data visualization and experimental
design.
Alignment with State-of-the-Art Research
The approach you’ve taken aligns strongly with current research trends in IoT and edge computing:
adaptive sampling, event-driven data transmission, and distributed intelligence. Here’s how:
Adaptive Sampling & Event-Driven Transmission: Rather than a fixed sampling rate, your system
adapts based on data and context. This concept is widely recognized as key to efficient IoT. For
example,   Agarwal   (2023)   explains   that   adaptive   techniques   adjust   sampling   or   transmission
frequency “based on the specific needs of the device or changes in the system of interest,” such as
sending data more frequently upon rapid changes and less frequently when conditions are stable.
Your implementation exactly follows this pattern: during stable periods, it sends infrequent averaged
readings, and during a disturbance (leak) it ramps up to real-time streaming. In essence, you are
doing  “sampling at transmission”  — the device always measures at 50 Hz, but only uploads high-
frequency data when an algorithm deems it necessary. This is a known strategy to reduce network
use while preserving important information: “if the sample is high entropy, upload data to the cloud...
[this]   reduces   network   bandwidth   utilization.”.   By   focusing   on   high-entropy   events   (like   sudden
pressure changes), your system ensures that “data is most useful when it brings information” – a
principle also noted in adaptive sensing research.
Dr. Atakan Aral’s Research Alignment:  Dr. Aral’s work heavily emphasizes efficient edge-cloud
operations and event-driven paradigms, which underpin your design. For instance, in a recent post
he describes treating IoT communication as a  “spiking process”  – sending data only when events
## •
## •
## 1

warrant it, rather than at a constant rate. This  “neuromorphic, event-driven approach significantly
reduces transmissions while preserving accuracy”. Your feedback-loop system is a clear realization of
that philosophy: it essentially sends bursts of high-rate data (50 Hz “spikes”) only when a leak event
triggers it, instead of a continuous 50 Hz stream. In other words, you’re implementing the idea that
“instead of sending data periodically or on simple thresholds,” the system adapts to context, much like
Aral’s  EdgeSynapse  project  which  achieved  sustainable  sensing  by  sending  data  in  an  event-
triggered manner. This alignment is a strong validation of your approach – you’re leveraging the
same fundamental strategy that recent edge computing research advocates for remote, resource-
constrained monitoring.
Trade-offs and Feedback Control:  Beyond   event-triggering,   your   system   uses   a   closed-loop
feedback control to balance trade-offs, which is also seen in literature. Aral et al. (2020) discuss
dynamically adjusting synchronization frequency in distributed analytics to manage the trade-off
between  timeliness  and  resource  usage.  Your  solution  analogously  adjusts  data  frequency  to
manage the trade-off between  fidelity (safety)  and  bandwidth (efficiency). By introducing a
controller that monitors both the application state (leak or no leak) and infrastructure state (CPU
load), you ensure the system adapts not just to the environment’s events but also to the system’s
capacity. This is more sophisticated than many IoT deployments, which often ignore resource-
awareness. The incorporation of a multi-priority fairness algorithm ties into the broader theme of
resilient IoT. In critical infrastructure research (including projects at University of Vienna), there’s an
emphasis on “resilient operation under load” – e.g., shedding load or degrading quality gracefully
when resources are constrained, and ramping up quality when resources are abundant. Your Priority
2 and 3 modes (CONSTRAINT and OPPORTUNITY) implement exactly that: under high load, you
sacrifice fidelity to keep the system stable, and under light load, you opportunistically collect extra
data for future benefits. This is a form of adaptive resilience that is very much in line with cutting-
edge systems research.
Similar Works in IoT and Water Monitoring: It’s worth noting how your work compares with others
addressing the same problem domain:
Pipeline Leak Detection Algorithms: Many recent papers focus on improving leak detection accuracy
using advanced algorithms, assuming data is available. For example, Saleem et al. (2024) use a deep
learning approach (CNN-LSTM) on high-frequency acoustic emission data to detect pipeline leaks,
achieving over 99% accuracy. However, methods like these typically require streaming large volumes
of sensor data to the cloud (acoustic signals, high-speed pressure readings, etc.). That’s exactly the
scenario where bandwidth becomes a bottleneck. Your system can be viewed as complementary: it
would allow something like the above deep-learning model to run or be applied without constantly
sending raw data – only when a suspected leak arises do you ship the full-resolution data (or one
could even run a lightweight ML model on the edge and send features). In effect, your architecture
makes it feasible to deploy high-fidelity analytics in a scalable way. It preserves the “forensic” data
quality on demand, which those accuracy-driven approaches require, but without incurring the cost
at all times.
Static Smart Sensors vs. Adaptive Sensors: Traditional smart water systems often use static
configurations. For instance, many smart water meters or pressure loggers might report data at a
fixed interval (e.g., 1 Hz or one sample per minute) to save power. This is analogous to your
Scenario 2 (Static Edge), which is extremely bandwidth-efficient but can miss fast transients.
Researchers have noted that while such static schemes save energy, they can lose critical
## •
## •
## •
## •
## 2

information between samples. Your work directly addresses that known blind spot by dynamically
switching to a high sample rate when needed. In academic literature, adaptive sampling schemes
have shown the ability to drastically cut down data volume while retaining accuracy – e.g., one study
achieved ~79% reduction in samples with minimal error by changing the sampling rate based on
signal analysis. Your claimed 98% reduction is even more dramatic, thanks to using an extreme ratio
(50 Hz vs 1 Hz) and only temporarily elevating the rate during events. This is a novel edge-case of
adaptive sampling – essentially binary modes (normal vs debug) rather than continuously tuning
the rate, which is a simple and practical design for an industrial setting.
Hierarchical Edge-Cloud Designs: The three-layer architecture (Edge – Broker – Cloud) you
implemented is very much consistent with modern fog computing architectures and has precedent
in research. The edge isolates and preprocesses data, a broker (or gateway) routes messages, and
the cloud aggregates global knowledge. This isolation is good for security and reliability. For
example, if connectivity is lost, edge nodes can continue operating autonomously (your system
indeed does this – the edge keeps detecting leaks locally even if the cloud connection drops, which is
a resilience point to highlight). Many academic projects stress that purely cloud-based IoT is risky
(latency, connectivity issues) and purely edge-based is limited (each node has a local view only). By
having a “local reflexes, global brain” setup, you are implementing what some recent works and
funding projects aim for. (As an aside, Atakan Aral is involved in research like sustainable water
management with IoT and edge AI for environmental monitoring, which advocate distributed
intelligence in exactly this manner. Your project can be seen as a concrete instantiation of those
principles in the water pipeline context.)
Bottom line:  Your implementation stands on solid ground, aligning with and even extending current
research. It blends ideas from adaptive sensing, edge computing, and distributed control. The inclusion of a
feedback loop for fairness is an innovative twist that not all prior adaptive sampling works consider (they
often trigger on data changes alone, ignoring system load). This could be a key contribution of your thesis –
a demonstration of how closed-loop control can make an IoT system both efficient and safe. As Dr. Aral’s
latest work suggests, event-driven data strategies are the future for sustainable sensing, and you’re
delivering a working prototype of that concept.
## System Architecture Evaluation
Your system is structured in three logical layers, each with clear responsibilities, which is ideal for this kind
of application:
Layer 1: Edge (Devices and Edge Agent) – “The Hands” of the system.
Deployment: a small computer (e.g., a Raspberry Pi or a K3s VM) on-site running the Edge Agent
software.
Function: Ingests raw sensor data at 50 Hz with minimal latency and performs first-line processing.
It runs a leak detection algorithm locally and decides what data to send upward. In normal mode it
sends aggregated 1 Hz summaries; in debug mode it forwards every reading (50 Hz); in economy
mode perhaps extremely sparse data (as you defined, one sample per 5 min). A crucial rule you
followed is that raw 50 Hz data never leaves the edge unless authorized – this ensures bandwidth
is conserved by default. This design also inherently improves security (sensitive high-res data isn’t
continually exposed) and privacy.
Local Broker: You smartly included a local MQTT broker on the edge (call it the “Edge Broker”) that
the sensor publisher and Edge Agent use internally. This means the sensor device can push 50 Hz
## •
## •
## 3

data to the local broker (fast, no network delay), and the Edge Agent subscribes to that. The Edge
Agent then publishes the processed data to the central broker in the cloud. This decoupling is
excellent: it means even if the cloud or network is down, the sensor-to-edge connection still flows
and the Edge Agent could potentially log data or at least keep detecting leaks. In other words, if the
internet fails, the edge still runs; the cloud just temporarily stops seeing updates. This is a
resilience feature that you should definitely mention in your defense.
Layer 2: Transport (Central MQTT Broker) – “The Nerves”.
Deployment: a central message broker (E.g., Mosquitto) on a VM or server reachable by both edge
and cloud components.
Function: Purely routes messages between publishers and subscribers. It connects the distributed
edges with the cloud in a decoupled way. This layer doesn’t do processing; it’s infrastructure. By
using MQTT topics (iot/data/plant-a and iot/control/plant-a etc.), you maintain a clean
separation between data streams and control commands. The naming (iot/data/{site_id}
and iot/control/{site_id}) is well-chosen for clarity. This central broker approach is standard
in IoT architectures and works well here. One broker can handle multiple sites (plant-a, plant-b, ...) if
needed, enabling scalability.
Layer 3: Cloud (Subscriber, Database, Controller) – “The Brain”.
Deployment: a cloud VM (or just a more powerful machine) running the data ingestion subscriber, a
time-series database (InfluxDB), and the control logic module.
Function: This layer has the global view and decision authority. The Subscriber component listens
to the iot/data/# topics (data from all sites) and writes the incoming data into the database.
Using InfluxDB here is a good choice for time-series data. The schema you defined is appropriate:
one measurement (e.g., water_pipeline) for sensor telemetry with fields like pressure, flow, etc.,
and another measurement (controller_decisions) for logging the controller’s outputs. The
Controller runs a loop (every 10 seconds as you noted) that queries the database for the latest state
and decides if any mode change is needed. Specifically, it checks for leak flags and CPU load. It then
publishes a JSON command to the control topic if a mode switch is warranted. This design cleanly
separates concerns: the edge never decides global strategy, it only handles local detection; the cloud
doesn’t handle raw data analysis (except in baseline scenario) but focuses on coordination. In system
design terms, it’s a classic sense-decide-act loop, where the cloud acts as the coordinator (decider)
and the edge as the actuator for data rate changes.
Fairness Algorithm: The priority hierarchy you implemented in the controller is a highlight of the system.
To recap, the priorities are:
-   Safety first (Leak) – If a leak is detected at a site, command that site to DEBUG (50 Hz), immediately and
regardless of bandwidth/cost. (This ensures no crucial data is missed during emergencies.)
-    System Constraint (Overload)  – If  no leaks  currently and cloud CPU > 80%, command all sites to
ECONOMY (ultra-low rate). (This prevents overload collapse by shedding load – a form of graceful degradation.)
-   Opportunity (Idle resources) – If no leaks and cloud CPU < 20%, you can afford luxury: command all
sites to DEBUG (50 Hz). (This leverages idle capacity to collect richer data, e.g., to improve ML models later –
turning downtime into a “free lunch” for data gathering.)
- Default (Normal) – If none of the above conditions, maintain NORMAL mode (1 Hz).
This algorithm is well thought-out and addresses both extremes (emergency and idle) as well as failure
prevention. It’s essentially a closed-loop control policy for a shared resource (bandwidth/CPU). In academic
## •
## •
## 4

terms, it ensures fairness and adaptability: critical events get top priority to bandwidth, and less critical
data backs off when needed. Notably, you also implemented hysteresis and a cooldown on mode changes,
which is very important. Without hysteresis, the system might oscillate (e.g., CPU hovering around 80%
could trigger economy then normal repeatedly). You chose, for example, 80% to enter economy and 60% to
exit, plus a 30s lockout. This means once it goes to economy, it won’t switch back until load drops well below
80% (to 60%) and at least 30s have passed. This prevents unstable flip-flop behavior. Including this shows a
high level of rigor in your design – something many similar implementations might overlook. It directly
addresses the thesis requirement about stability (“must not oscillate unstably”).
Security & Isolation: Another positive aspect – the architecture inherently has some security isolation. The
edge broker can be isolated behind firewalls, and the cloud only sees what the edge publishes. The cloud
cannot directly poll the sensor; it only sends high-level commands. This reduces the attack surface on the
physical device. Also, using MQTT with strict topics means you can enforce permissions (e.g., edges publish
only to their data topic; only the controller publishes to control topics). If not already, you should consider
basic authentication for MQTT and perhaps TLS encryption, since this is critical infrastructure data. It wasn’t
explicitly mentioned, but for a real deployment it’s crucial. For the thesis prototype, simply noting that it’s
possible to secure it might suffice if you didn’t implement it fully.
Comparison to Alternative Architectures: It’s useful to contrast what you built with the two extremes: -
All-Cloud (Baseline Scenario 1) – The sensor streams 50 Hz directly to cloud, and cloud does everything. This is
simple but as you identified, it has high latency (data travels to cloud for analysis) and enormous bandwidth
usage. If network fails, you lose all oversight. Your baseline scenario essentially mimics this to have a
reference point (we’ll evaluate results later). -  All-Edge with No Feedback (Scenario 2)  – The sensor is
connected to an edge agent that aggregates (1 Hz) and there’s no cloud control or adaptation. This is very
bandwidth-efficient and the edge can detect leaks quickly locally, but the cloud (and central database) only
ever see 1 Hz data. A fast transient leak could occur and reseal between those one-second samples and be
“averaged out” of existence in the record. That means forensic data is lost – later analysis on the cloud or
any machine learning model would have gaps. Many modern “IoT at the edge” deployments run into this
issue: they preprocess so much that the central system gets a very limited picture.
Your  Adaptive Architecture (Scenario 3)  combines the strengths: normally it behaves like the efficient
edge case (98% bandwidth reduction), but when a leak happens it behaves like the cloud case (full fidelity
data). And if the system is under stress, it even goes beyond static edge (dropping to an even lower rate) to
protect itself. This “best of both worlds” claim is exactly what you’ll verify with data. Architecturally, there’s
little to criticize here – it’s robust and conceptually sound.
One thing to ensure:  Correct integration of Baseline mode vs your infrastructure.  In Scenario 1
(Baseline), you bypass the Edge Agent. Likely you have the sensor publisher send 50 Hz to the central
broker, and the cloud subscriber writes it to Influx. The cloud controller is turned off in this scenario (or at
least, it should not send any control commands). Also, leak detection in baseline is done at the cloud side.
You mentioned in your docs that baseline’s leak detection is cloud-based with high latency. That implies you
did   not   use   the   sensor’s  leak_flag  field   for   immediate   detection.   (If   the   sensor   still   publishes
leak_flag=1 in baseline, the cloud could see it immediately, which would actually make detection fast –
undermining your latency comparison. I assume in baseline run, either the leak_flag was not included
or the cloud ignored it and instead you simulate detection by some slower means, e.g., cloud noticing a
sustained pressure drop after a few seconds.) This detail is important for a fair comparison: be prepared to
explain how baseline detection time is defined. Perhaps you required “two consecutive seconds of pressure
## 5

below X” or some criterion, which naturally introduces a few-second delay to avoid false alarms. That would
justify the “>5 s” detection latency for cloud. In contrast, your edge agent likely flags a leak within 0.02–1 s
of   it   occurring   (essentially   immediately   on   the   first   threshold   breach).   This   separation   of   detection
responsibilities is properly done – it ensures you’re not circularly using the ground truth flag on cloud side.
In summary, the system architecture is well-designed for the thesis goals. Each layer is doing the right
job, and the feedback loop strategy is clearly implemented. It adheres to  distributed system best
practices (decoupling, local autonomy, eventual consistency between edge and cloud) and meets security/
reliability concerns by design.
Data Flow, Telemetry and Implementation Details
Understanding the data that flows through your system is crucial, both for verifying correctness and for
later visualization. Let’s break down what data is sent, stored, and acted on, and evaluate if it’s suitable:
MQTT Topics & Messages: You established a clear topic hierarchy:
iot/devices – local topic for raw 50 Hz sensor data (device → Edge Agent internally). This doesn’t
go to cloud.
iot/data/{site_id} – uplink topic for processed data (Edge Agent → Cloud). This carries
whichever data the edge decides to send.
iot/control/{site_id} – downlink topic for commands (Cloud Controller → Edge Agent). Used
to tell the edge to switch modes.
Each site_id (plant-a, plant-b, etc.) denotes a distinct deployment. Even if you only simulate one
site (plant-a), having this in the design is good for extensibility. It means the cloud could manage
multiple sites with independent leak events or resource needs – something future work or another
student could expand on.
Raw Sensor Data Schema: At 50 Hz, your sensor (or simulated sensor) publishes JSON messages
like:
## {
## "device_id":"device_001",
## "timestamp": 1705593102.123,
## "pump_status": 1,
## "valve_position": 45.5,
## "tank_level": 68.2,
## "pressure_psi": 118.4,
## "flow_gpm": 42.1,
## "leak_flag": 0
## }
This is quite comprehensive. It includes:
## •
## •
## •
## •
## •
## 6

Device metadata: an ID and timestamp. (The timestamp presumably is device-side; you might also
get an InfluxDB server-side timestamp on insert, but using the device’s time ensures alignment if
needed).
Actuator states: pump_status (on/off) and valve_position (% open). These indicate the operational
state of the system (e.g., a pump might be on to pressurize the pipe, a valve might be modulating
flow).
System measurements: tank_level (% full), pressure (psi), flow (gallons per minute). These are key
physical metrics. Pressure is typically the critical one for leak detection (a sudden drop or unusual
fluctuation can indicate a leak). Flow can also indicate leaks (unexpected outflow), and tank level
might drop if water is lost. Including all three gives a fuller picture – good for analysis (for instance,
seeing pressure drop accompanied by flow increase could confirm a burst).
Leak ground truth flag:leak_flag which is 0 or 1 indicating if a leak is present at that exact
moment in the simulation. This is an artificial field (in a real sensor you wouldn’t have this – it’s only
known in simulation). It’s very useful for evaluation: you use it to measure detection latency and to
ensure the edge detection logic works (the edge agent can set its own internal leak flag by analyzing
the sensor data; in simulation, you might cheat by having the sensor set this true during a scripted
leak event, but the edge agent should be able to infer it even without the flag by checking pressure,
etc.). In your edge aggregation, you actually propagate this as a safety measure, which I’ll cover next.
Edge Agent Processed Data Schema: When the Edge Agent sends data to cloud (topic iot/data/
plant-a), it sends a JSON like:
## {
## "site_id":"plant-a",
## "timestamp": 1705593103.000,
## "pressure_psi": 118.2,
## "flow_gpm": 41.9,
## "tank_level_pct": 68.3,
## "valve_position": 45.5,
## "pump_status": 1,
## "leak_flag": 1,
"mode":"NORMAL"
## }
A few important points here:
The values like pressure, flow, tank_level are aggregated/filtered values. In NORMAL mode, you
said it’s the average over the last second (50 samples). In DEBUG mode, presumably the agent
doesn’t average – it might just forward each raw sample but possibly still in this schema. In
ECONOMY mode (5 min), it would average over a 5-minute window. This approach is fine. Averaging
is a simple filter; it smooths out noise and reduces data volume. (One minor consideration:
averaging introduces a 0.5 s delay in representing the data, since you collect a full second of samples
then report the mean. But 1 s resolution is still real-time enough for this context, and during a leak
you switch to raw anyway.) You might clarify in your thesis that in DEBUG mode, the “aggregated”
## •
## •
## •
## •
## •
## •
## 7

message is effectively every 0.02 s and equivalent to raw (the code likely just shortcuts and
forwards raw readings marked as mode=DEBUG).
For valve_position and pump_status, you wisely chose to take the last known value in that interval
rather than an average. Averaging something like a binary on/off or a percentage setpoint could be
misleading (e.g., averaging pump_status 0 and 1 would give 0.5 which means nothing). Last-known
(or majority for binary) is the right approach. In practice, these might not change often anyway.
Leak flag propagation: You set leak_flag in the uplink as the max value over the interval. This
means if any of the 50 raw samples in that period had leak_flag=1 (i.e., the leak occurred), the
aggregate message gets leak_flag=1. This is crucial. It ensures that even if a leak was brief and got
averaged out of pressure/flow, the cloud is still alerted that “a leak was detected in this period.” In
Scenario 2 (static edge without feedback), this is the only way the cloud would know something
happened. It might not have the full waveforms, but at least a flag flips from 0 to 1. This could
trigger an alarm to send someone on site, etc. So including leak_flag is a safety net. In Scenario 3
(adaptive), the moment the edge detects a leak, it will send a message with leak_flag=1 and switch to
DEBUG mode subsequently. The cloud thus gets an immediate heads-up and then the detailed data.
This two-step is excellent for safety: first notify, then provide details.
Mode field: You include the current mode (“NORMAL”, “DEBUG”, or “ECONOMY”) in each message.
This is very helpful for debugging and analysis. It lets the cloud (and your Grafana dashboards) know
what mode the edge thought it was in at that time. For instance, in a time-series you could see
mode flip from NORMAL to DEBUG at the leak moment. In InfluxDB, however, storing a string field
for mode is less convenient for querying. You addressed this by also having a mode_numeric field
in   the   DB.   Likely   you   map   NORMAL=0,   DEBUG=1,   ECONOMY=2   (or   something   similar)   for
mode_numeric. This will allow you to do time-series queries on mode (e.g., count how many
seconds in each mode, or create a graph of mode over time). Good foresight on data schema design
there.
Database (InfluxDB) Schema: You outlined it as:
Measurement: water_pipeline – tagged by site_id (and host – host probably denotes the
machine   that   wrote   the   data,   e.g.,   cloud-node,   which   is   fine),   and   fields:  pressure_psi,
flow_gpm, tank_level, leak_flag, mode_numeric. This will store a time-series point for
each message the cloud subscriber writes. Given your data rates, this measurement in Scenario 3
will have mostly 1 point/sec, with bursts of 50 points/sec during debug periods. InfluxDB can handle
that pattern; just ensure your Influx retention and shard duration are reasonable (50 Hz is not super
high, but if you run long experiments it can accumulate). Also make sure leak_flag is stored as
an integer or boolean field, not a string (likely it’s an int 0/1). That way you can use it in queries (e.g.,
find times where leak_flag=1).
Measurement:  controller_decisions  –   tagged   by  site_id,   fields:  decision_enum,
cpu_load. This is an audit log of each time the controller runs and issues a decision. For example,
every 10 s you might log something.  decision_enum  could indicate what decision was made
(perhaps 0 = no change, 1 = set normal, 2 = set debug, 3 = set economy, or maybe it directly logs the
mode command). And  cpu_load  presumably records the CPU at that time. I really like this
addition: it means you have data to prove why and when the controller did something. In the thesis
defense, if asked “How do we know the feedback loop actually kicked in as intended?”, you can pull up
this log and show: “At time T, leak_flag went 1 in Influx, and at the same time the controller_decisions log
shows a DEBUG command issued (decision_enum corresponding to DEBUG) with CPU reading X%. Then a
bit later, at time T+Δ, it logged a NORMAL command once the leak passed.” This directly demonstrates
## •
## •
## •
## •
## 8

the closed-loop behavior. It’s also useful for analysis: you can verify the controller followed the
priority rules (e.g., if both a leak and high CPU happened, the log should show it chose the leak-
driven debug mode, ignoring CPU – you could check that decision against the inputs).
Monitoring Data (CPU, etc.):  You mentioned using Node Exporter + Telegraf to monitor CPU,
memory, etc., on various components. It sounds like you are collecting system metrics for the cloud
node (and maybe the broker and edge nodes). This is great for getting a complete picture of
resource usage. If Telegraf is pushing these to InfluxDB, you’ll have a measurement like cpu  or
system  with   fields   like  usage_idle,  usage_system,   etc.   If   using   Prometheus   via   Node
Exporter, you have those metrics in Prometheus. Either way, you’ll want to visualize CPU usage to
demonstrate the effect of your adaptive algorithm. Since you also log cpu_load in the decisions,
there might be slight duplication, but the decision log likely samples CPU every 10 s whereas Node
Exporter might be more frequent. Using the more granular data could show, for example, how CPU
went high and then after the system dropped to ECONOMY mode, CPU stabilized. It’s up to you, but
ensure consistency (maybe log CPU% from the same source the controller uses, to avoid confusion).
It’s implied the controller queries Prometheus for node_cpu. If that’s the case, you could either (a)
also configure Prom as a Grafana data source to draw those graphs, or (b) push that CPU data into
Influx via Telegraf (since you already have Telegraf, it can scrape Prom or Node Exporter and write to
Influx).   The   simplest   might   be:   Telegraf   collects   CPU%   and   writes   to   InfluxDB   measurement
“cpu_load” with tags for each host. Then your controller could query Influx (instead of Prom) for the
latest CPU. But this detail aside, you do have the needed data to confirm the feedback loop’s
effect on system load, which is a strong point.
Deterministic Simulation & Reproducibility: You implemented deterministic seeding for the leak
event timing (LEAK_SEED=42). This is excellent for experimental repeatability. It means every run
of the simulation (for each scenario) will inject the leak at the same simulation time (say at exactly
T=300s, as your note suggests). Thus, when comparing scenarios, they all deal with a leak at that
same moment and of the same magnitude. This ensures a fair apples-to-apples comparison. Not all
theses ensure such consistency, so it’s a notable strength. It lets you justifiably say differences in
outcomes are due to architecture differences, not random variation.
Leak Detection Logic: Just to verify: in Scenarios 2 and 3, the edge is responsible for detecting leaks.
How does it do this? The simplest way is thresholding – e.g., if pressure drops by more than X psi
within Y milliseconds, set leak_flag=1 (and possibly start debug mode). Since your simulation
conveniently has a  leak_flag ground truth, perhaps you simply use that: i.e., the Edge Agent
could subscribe to the raw data and if it ever sees leak_flag=1 in the raw message, it knows a
leak is happening (that’s almost like cheating, but in simulation it’s equivalent to a perfect detector).
In a real deployment, you’d replace that with logic on pressure/flow values. For the thesis, it’s okay to
use the ground truth flag to trigger the mode switch (just mention that it simulates a detection
algorithm  that  would  sense  the  pressure  anomaly).  The  important  part  is  the  separation of
concerns: Baseline’s cloud does not get to magically use this flag for instant detection; only the edge
uses it. And indeed your documentation clarifies that to avoid a circular definition of “detection” you
kept baseline and edge detection separate. This is methodologically sound.
Data Integrity and Timing: Replacing time.sleep() with a precise monotonic loop in the sensor
code to achieve true 50.00 Hz sampling was a good move. It ensures your baseline scenario really
sends data at a consistent high rate (and that 1 Hz means exactly one second intervals, etc.). This
## •
## •
## •
## •
## 9

adds credibility: you can say  “the baseline wasn’t just an approximation; it truly represents a 50 Hz
industrial sensor feed, as we enforced 20 ms intervals precisely”. Small details like this strengthen the
experimental validity.
Overall, the data pipeline is well-designed. The  content of the data  is rich enough to allow thorough
analysis (you have measurements, state flags, and metadata). For Grafana and analysis purposes, you
might not plot every field (for instance, valve_position might not change in your scenario if you kept it
constant, and pump_status might just be always 1 if the pump stays on throughout). If those don’t vary,
they’re not very interesting to graph. They were included likely for completeness or future scenarios (maybe
you planned scenarios where pump turns off or valve closes, etc., but if not, they can be ignored in
visualization). The primary fields of interest will be pressure (as the key indicator of leaks), possibly flow   (to
see if leak causes an outflow change), and the boolean leak_flag (for ground truth reference). The mode
and cpu_load are also important to show the system’s adaptive response.
One suggestion:  consider logging an explicit event when a mode change occurs.  You do have the
controller_decisions log, which essentially does this (each entry presumably reflects a decision, including
mode changes). Grafana can use that for annotations. Another idea is the edge agent could publish its
current mode periodically or on change (even just to the data stream or a separate topic). You included
mode in each data point, so that’s effectively doing it continuously – which is fine.
Another small suggestion: since you have multi-variable data (pressure, flow, level) which might have
different ranges, ensure you handle units and scaling in Grafana appropriately (e.g., psi vs gpm vs %). It
might be better to plot them in separate panels to avoid confusion.
Experiment Scenarios and Data Presentation
You will be comparing three scenarios to validate the hypothesis: Baseline (cloud-only 50 Hz), Static Edge
(1 Hz fixed), and Adaptive Edge (dynamic 1↔50 Hz with feedback). Let’s talk about what data to collect and
how to visualize the comparisons to conclusively demonstrate your system’s advantages. Remember, as
your notes say, the goal is to show Scenario 3 achieves efficiency close to Scenario 2, and safety/fidelity close to
## Scenario 1.
- Bandwidth/Efficiency Comparison:
Metric: Volume of data sent to cloud over time (or total bytes/messages over the whole test).
Expectation:  Scenario 1 (Baseline) uses 100% bandwidth (the reference), Scenario 2 uses only ~2%, and
Scenario 3 averages maybe 2–5% with brief spikes.
A great way to visualize this is a time-series plot of data rate (messages per second, or kilobytes
per second) for each scenario. For instance, you could take the InfluxDB data and compute how
many points per second arrived in each scenario. In Grafana, you might do this by a COUNT()
group-by time on the measurement. Alternatively, your script can count MQTT messages. The result
would be something like: a graph where Baseline is a flat line at 50 msg/s, Static is a flat line at 1
msg/s, and Adaptive is 1 msg/s most of the time, with a jump to 50 msg/s during the leak (and
maybe back to 1 after). This clearly shows the “spiky” nature of adaptive. In fact, it will look almost
like a binary switch graph: low, then high, then low – reflecting exactly the feedback activation.
## •
## 10

You can also calculate total data transmitted in each run (e.g., Baseline sends 50duration points, Static
sends 1duration, Adaptive sends roughly (mostly 1duration + extra during leak)). Presenting those
numbers: “We sent ~50,000 readings in baseline vs ~1,200 in adaptive over the same period – a 98%
reduction.”* If using bytes, include that as well (maybe baseline = X MB of data vs adaptive = Y MB).
This backs the efficiency claim quantitatively.
Cite comparison: Other adaptive schemes in literature often report such savings. You can note that
your achieved reduction (~98%) is higher than typical because you aggressively minimize data when
idle, leveraging the fact that leaks are rare. (For context, recall one study saved ~47% or 79%
depending on method – your savings are even greater by design.)
- Data Fidelity/Safety Comparison (Leak Event):
Metric: Quality of data available to the cloud around a transient event (leak).
Expectation: Baseline and Adaptive will have detailed high-frequency data during the leak; Static will have
only coarse data and might miss the event’s shape (possibly only a flagged anomaly without detail).
To show this, it’s effective to plot the pressure (and/or flow) vs time around the leak moment for each
scenario: - Take a window, say 10 s before through 10 s after the leak starts. Plot the pressure readings that
the cloud/logged in each scenario over that period. - Baseline: you’ll have 50 Hz data, so it will show a rapid
drop (or spike) and rich detail (maybe oscillations or a specific profile of the event). - Static Edge: you’ll have
one reading per second. It will likely show a much smaller blip. If the leak happens and is resolved within,
say, 0.5 s, the static edge’s next 1 s sample might show only a slight change (since it averaged a half-second
of normal and a half-second of low pressure, for example) or even no significant change if the leak was
extremely brief. Even though your static agent sets leak_flag=1 for that aggregate, the pressure value
itself might not look alarming. Essentially, scenario 2 might  detect  (via the flag) but not  record  the full
magnitude of the pressure drop. This is your  “blind spot”  to highlight. - Adaptive: should overlap with
baseline for the duration of the leak. In adaptive, the edge would switch to 50 Hz as soon as leak is
detected, so the cloud data from that point onward is identical (or nearly) to baseline’s. There may be a
slight delay (tens of milliseconds perhaps) between the leak start and the controller command arriving
(especially if using MQTT over network). But since the edge itself can detect and start sending raw
immediately (depending on how you coded it, it might even start forwarding raw data before the cloud
command if the edge autonomously decides to go debug mode on leak – but I think your design was that
the cloud decides, not the edge autonomously. This means there could be ~100 ms network + controller
loop delay). Regardless, the adaptation is fast enough that you’ll capture most of the event. If you find a tiny
delay, you could mention it: e.g., “leak started at t=300.00s, edge detection at t=300.01s, cloud received high-
rate data by t=300.1s”, which is still far superior to waiting 5 s in baseline cloud detection.
A figure showing these plots can be very powerful: maybe use different colors for each scenario’s pressure
curve. Or three subplots stacked for direct visual comparison. The key outcome: Scenario 2’s curve is much
smoother/flattened, potentially missing the peak/trough, whereas Scenario 3’s curve matches
Scenario 1’s in capturing the event shape. This proves that adaptive mode retained the forensic detail
just like a full-bandwidth system would, thereby “beating” the static edge on safety.
Additionally, mention that in static scenario the cloud only knows leak happened because leak_flag went
to 1  , but it has no further data at high resolution. In adaptive, leak_flag goes 1 and high-res data follows,
and in baseline leak_flag (if it were present) or anomaly is seen with high-res data anyway. This addresses
the forensics aspect: scenario 3 and 1 are “good” forensics, scenario 2 is “poor”.
## •
## •
## 11

## 3. Detection Latency Comparison:
Metric: Time from leak occurrence to detection/response.
Expectation:  Edge-based   detection   (Static   Edge   and   Adaptive)   <   1   s;   Cloud-only   detection   (Baseline)
significantly higher (e.g., ~5 s).
If you logged when detections occur, you can extract this. For edge, you might say detection is essentially at
t_leak (maybe within one cycle of the 50 Hz loop, so 20 ms to 1 s max). For baseline cloud, if you defined it
needed e.g. 5 consecutive 50 Hz samples out-of-bound or a 5-s moving window, then that’s ~5 s. You can
present this as: - A bar chart with two bars: Cloud Detection = 5 s,  Edge Detection = 0.05 s (for example). Or
three bars if you want to separate static vs adaptive, but they should be nearly identical since both detect at
edge. If anything, adaptive might detect slightly faster because the edge sees raw data at 50 Hz, whereas in
static edge scenario, the edge also sees raw 50 Hz (the edge agent does, even if not sending it). Actually,
yes, in static scenario the edge agent still sees 50 Hz internally and would detect just as fast (and it sets
leak_flag in the 1 Hz message immediately). So detection latency for static and adaptive are both ~0.x s in
practice. It’s just that static doesn’t send raw data up. - Another way: a timeline diagram showing when leak
happens vs when each system raises an alarm. But a simple bar or even just stating the numbers in text
might suffice if time is short. However, since your alignment doc explicitly mentions a “Latency bar chart”,
you likely will include one. So yes, perhaps do a small bar graph panel in Grafana or via the script.
Be prepared to justify why cloud took 5 s – typically, one would say  “to avoid false positives, the cloud
algorithm required the pressure to be continuously low for a few seconds before declaring a leak.” Meanwhile,
the edge, being local, could use a more sensitive threshold or detect the immediate spike/drop (and in any
case, a false positive at edge doesn’t cost much – it just triggers a brief data surge, which is acceptable
given the stakes). This highlights a subtle point: edge detection can be more aggressive (faster, maybe
even at risk of a tiny false alarm) because the cost of a false alarm is low, whereas cloud detection might be
conservative to avoid crying wolf over noisy data. This further supports moving detection to edge.
## 4. Resilience Under Cloud Load:
Metric: System behavior under high CPU or network load; does it maintain critical functionality?
Expectation: In an overload, Adaptive (Scenario 3) will throttle data (enter ECONOMY mode) to alleviate load,
whereas Baseline (Scenario 1) will continue full blast and potentially collapse or backlog. Static (Scenario 2)
already sends minimal data, so it’s relatively safe by default (and edge does processing locally, so cloud load
is low anyway in that case).
To test this, you can simulate a heavy load on the cloud. For example, start a CPU stress test or run some
computation during the experiment to push cloud CPU > 80%. Your controller_decisions log and Grafana
should then show a switch to ECONOMY mode. Key things to visualize: - Cloud CPU % over time (from your
monitoring data). Show that it exceeds the 80% threshold at some point. - Mode changes corresponding to
that. Ideally, an annotation “Entered ECONOMY mode” at that time. Or a panel showing mode_numeric
dropping to the economy state. - Data rate drop: When mode goes to ECONOMY, you expect the data
throughput to drop from 1 Hz to, say, 0.003 Hz (one sample per 5 min). In a short test, you might not even
get another data point soon, but you can show that effectively data nearly stops. If your test isn’t long
enough to wait 5 min, perhaps you set economy to 10 s for demonstration – but the docs say 5 min.
Regardless, the concept is: the system “shed load” when overloaded. This is analogous to techniques in
networking (like TCP congestion control) or in real-time systems (graceful degradation). It proves stability. -
If possible, illustrate that baseline under the same conditions would overwhelm the CPU. For instance, you
might have logged CPU in baseline scenario as well – maybe it hit 100% trying to handle all inserts, causing
## 12

data lag. If you observed any data loss or lag in baseline due to overload, mention it. (If not, one could
reason: baseline would require a much more powerful cloud to handle long-term 50 Hz from many sites; if
under-provisioned, it fails – whereas adaptive adapts to what is available.)
Since static edge always sends so little, it likely never overloads the cloud even if CPU is limited (unless the
cloud CPU is extremely weak). So static is inherently resilient to CPU overload in terms of not adding load.
However, static can’t adapt upward to use available resources – it’s efficient but not adaptive. Your adaptive
system covers both angles by design.
- Stability of Mode Transitions:
Metric: Frequency of mode switching; absence of oscillations.
Expectation: The adaptive system should not rapidly flip-flop modes; with hysteresis, it will stick to a mode
for   a   reasonable   period.   You   might   see   at   most   a   few   mode   changes   in   a   scenario   (e.g.,
Normal→Debug→Normal for a leak, and maybe Normal→Economy→Normal for a load spike).
To demonstrate this, you could simply count the number of mode changes during the test. For example,
“During a 30 min run, the adaptive system switched mode 3 times (Normal→Debug at leak, Debug→Normal after
leak, Normal→Economy during overload, Economy→Normal after overload).” That’s 4 transitions total, which is
quite low. If you had no hysteresis and jittery conditions, you might have seen dozens of switches (which
would indicate instability). A bar chart comparing “transitions with hysteresis vs without” could be overkill
unless you specifically simulate a without-case. Instead, you might present a timeline diagram or table: Leak
at 5 min caused one switch to DEBUG, back to NORMAL at 6 min; CPU overload at 10 min caused one switch to
ECONOMY, back to NORMAL at 15 min, etc. The controller_decisions log is actually the evidence here – it will
show time and mode decisions. You can point out there that it didn’t, say, bounce between DEBUG/NORMAL
repeatedly at any point. This satisfies the requirement that the control loop is stable.
Using Grafana, a state timeline panel (discrete bar over time) for the mode is a nice visualization. It would
show long solid bars of “NORMAL”, a short bar of “DEBUG” during the leak, back to “NORMAL”, maybe a bar
of “ECONOMY” during overload, etc. If you don’t have a plugin for that, plotting mode_numeric with step
interpolation would similarly show the hold times.
## Grafana Dashboard Design:
You expressed some confusion with Grafana after switching from the temperature/humidity example to this
water pipeline scenario. The key is to display the data in a way that highlights the feedback loop and
differences between scenarios, without overwhelming the viewer. Here are recommendations:
Use Multiple Panels: Given the variety of data (pressure, flow, tank, leak flag, mode, CPU...), it’s best
to split these into multiple panels for clarity. For example:
Pressure vs Time Panel: Plot pressure_psi over time. If you are showing a single scenario run, just
show that scenario’s data. If you are trying to overlay scenarios, you would need to have them in the
data source (e.g., runs at different times or different site tags). It might be easier to use Grafana to
show just one run dynamically, and use your Python script for static comparisons. On the pressure
graph, you could also plot flow_gpm if the scales are similar – but since PSI and GPM are different
units, it might be better to keep flow on its own panel or use a dual-axis. Perhaps simpler: focus the
primary graph on pressure, since that’s most indicative for leaks.
Leak Indicator: You can visualize leak_flag on the pressure graph by using it as a second series
with Y-axis on right (0/1). For instance, show leak_flag as a red line or points (value 1 during leak). Or
## •
## •
## •
## 13

use Grafana’s threshold/region feature to highlight the period of leak. Another method is to add an
annotation in Grafana at the moment of leak (since you know when you injected it). Even without
the ground truth, the system’s own data can indicate it (e.g., the first point with leak_flag=1, or the
controller decision to go DEBUG). Grafana allows querying a data source for events to annotate
charts. You could have it mark “Leak detected” when controller_decisions logs a DEBUG mode due to
leak.
Mode Timeline Panel: If possible, use a Discrete plugin or State timeline panel to map
mode_numeric to text (“NORMAL/DEBUG/ECONOMY”) and show the mode over time as colored
bands. This immediately shows the behavior of the feedback loop. If a plugin is not available or not
allowed, a workaround is to use a standard graph panel: plot mode_numeric as a stepped line
(Grafana has “stairs” interpolation) and manually set Y-axis values 0=ECONOMY, 1=NORMAL,
2=DEBUG, and maybe create Y-axis value mappings to labels. It’s not as pretty, but it works. The
viewer can then see at what times the mode value changed.
Flow and Tank Panels: If flow rate and tank level are relevant to your discussion (for example, maybe
the leak causes a sudden drop in tank level or spike in flow), show them in separate smaller panels. If
they don’t add much insight, you can omit them to keep the dashboard cleaner. It’s better to focus
the narrative: e.g., “we mainly use pressure to detect leaks, while flow and tank are secondary”. You
can always mention that those were monitored and could be used for cross-check or future work
(like detecting leaks via mass-balance using tank and flow).
CPU Load Panel: Show CPU usage (%) of the cloud node over time. If you have multiple sources
(cloud, broker, etc.), you might show just the cloud for simplicity, since that’s where the controller
and database run. Mark on this chart the 80% and 20% thresholds (Grafana can draw horizontal lines
or you can just note them). Then it will be clear that when CPU crosses 80%, shortly after the mode
goes to ECONOMY; when it dips below ~60%, the system goes back to NORMAL. If using
Prometheus, you might directly query node_cpu for idle vs total to get usage. If using
Influx+Telegraf, query the cpu  measurement (perhaps SELECT mean("usage_system" +
"usage_user") or simply 100 - mean("usage_idle") over an interval).
Network/Bandwidth Panel (if needed): You could also plot network usage if Telegraf captures interface
stats, but since we can derive message rates from the data itself, this might be redundant. Probably
not necessary for thesis, unless you want to explicitly show bytes/sec on the interface. The message
count plot we discussed earlier is effectively showing bandwidth in logical terms.
Dashboard per Scenario vs Combined: Because you cannot run all three scenarios simultaneously
on one system (and you mentioned you can only run one at a time), a live Grafana dashboard can
typically only show one scenario’s data (the one currently running). To compare scenarios, you have a
few options:
Sequential Runs on a Timeline: You could run Scenario 1, then immediately run Scenario 2, then
Scenario 3, one after another, while logging to the same InfluxDB (but maybe with different site_ids
or a field indicating scenario). For example, you could use site_id = "baseline" for run1, then
"static" for run2, "adaptive" for run3. If you do that, you can query in Grafana to overlay,
because the data is distinguished by tag. However, the time axis will be continuous (you ran them
sequentially). You could align them by time offset in a script later, but Grafana won’t easily “offset”
time for you. Alternatively, you could start all three modes concurrently by simulating three different
site_ids in parallel, each configured differently. That would truly generate data for all scenarios at the
same time, and you could compare their outputs directly in Grafana by selecting each site. This
## •
## •
## •
## •
## •
## •
## 14

might be tricky but not impossible (you’d basically run three publisher/edge sets, one with edge
agent bypassed, etc.). It might not be worth the complexity for a live demo, but for data gathering
it’s an idea.
Use the Python analysis script for comparisons: Your generate_thesis_figures.py likely
does exactly this: it probably queries data for each scenario (maybe from CSV logs or Influx queries)
and then produces comparative plots, like overlayed lines or bar charts. Rely on that for the final
figures in your thesis document or slides. Grafana can be used during the presentation to
demonstrate the system behavior in one scenario (adaptive) – which is compelling to watch live –
but for quantitative comparison, a pre-made figure is clearer.
One approach is: run all three scenarios with identical conditions, export their data (influx query or your
script collects it), then produce the 3-scenario comparison plots. Those plots would include e.g. the
bandwidth usage comparison and the leak zoom-in comparison we discussed.
Grafana Live Demo: If you plan a live demo of the adaptive system, have Grafana show the adaptive
scenario in action. For instance, set up a dashboard focusing on adaptive: show pressure in real-
time, and have an indicator or text showing current mode (Grafana’s single-stat panel could even
show mode as text if you feed it the latest mode field). Show CPU too. Then trigger your leak (if you
can manually trigger it or know when it will happen), and watch Grafana display how mode switches
to DEBUG, data frequency increases (you’ll see the pressure line update more rapidly perhaps, and
maybe you’ll see more data points or a change in chart resolution). Then trigger a CPU overload
(maybe start a CPU burn job) and watch mode go to ECONOMY (you’d see pressure updates slow
down – which might even look like the graph flatlines since few new points). This live visualization
will make the feedback loop tangible to the audience. Just ensure the timing is right (you might
speed up the demo by using a shorter period for economy mode if waiting 5 minutes is impractical,
or manually simulate CPU usage via a quick script when needed).
Grafana Limitations: Be aware that Grafana is not great at analysis across separate experiments –
that’s where your scripting comes in. Grafana excels at monitoring a running system or examining
one dataset at a time. Use it for what it’s good for (interactive display, real-time observation,
illustrating the timeline of one run). Use offline analysis for computing aggregates (like total data
sent) and combining multiple runs.
Improvements and Considerations
Finally, let’s cover a few things that you might refine or that similar research projects handle, to ensure you
have all angles covered:
Multi-Site Scalability: Your current tests are likely with one simulated site (one sensor stream). The
architecture does support many sites (it’s in the design), but you might not have tested multiple
concurrently. If time permits, consider demonstrating two sites simultaneously: e.g., plant-a with a
leak, and plant-b without a leak. The controller’s fairness policy would then, according to Priority 1,
set plant-a to DEBUG (because it has a leak), while plant-b could remain Normal or go Economy if
CPU is an issue. This would show that non-affected sites don’t unnecessarily jump to high-rate
(unless idle mode triggers them) and that the controller can target commands per site. If it’s too late
to do this, it’s fine – you can state the system is extensible to N sites and discuss how the controller
would handle competing leaks (e.g., if two leaks happen, both go DEBUG unless CPU is so limited
## •
## •
## •
## •
## 15

you have to drop one – an interesting scenario, though not in scope perhaps). Other research (like
Aral’s work on distributed monitoring) often consider multiple nodes impacting each other, so it’s
good to at least mention that dimension.
Energy Impact: One thing not explicitly measured in your current setup is energy consumption on
the edge device (since it’s likely a VM or Pi powered by outlet). But adaptive sampling is very much
about saving energy for battery-powered IoT devices. In your case, the sensor/publisher still runs at
50  Hz  all  the  time  (so  the  sensor  node’s  own  consumption  is  not  reduced  by  adaptation  at
transmission). However, if the radio (WiFi/LTE) is only transmitting at 1 Hz most of the time, it will
save some energy on communications. If the edge device were battery-powered, running the
network less often and sending fewer packets would indeed save energy. Similar research in
agriculture IoT found ~11% energy savings by using adaptive sampling on humidity sensors. Your
approach could theoretically save even more if the radio can power down between transmissions. It
might be worth mentioning as a side benefit: “This strategy isn’t just about bandwidth – it could also
prolong battery life on wireless sensors, since the communication module is active far less often. Prior
work in adaptive IoT sampling showed significant energy gains with only minor loss in data quality.” This
could broaden the impact of your work beyond just this water scenario.
Advanced Detection & False Alarms: As noted, some papers use fancy algorithms for detection
(like   deep   learning   on   sensor   data)   or   statistical   methods   (spectral   analysis,   etc.).   Your
implementation used a simple threshold/hysteresis for leak detection (presumably). That’s fine given
the focus, but an improvement could be incorporating a slightly smarter edge detection – for
example, if you had time, you could implement a simple anomaly detection that looks at pressure
trends or uses a change-point detection. This might reduce false triggers if, say, a benign pressure
fluctuation occurs. Currently, since you know exactly when the leak happens in simulation, false
alarms are probably not an issue. But consider if the sensor noise spiked the pressure for a moment,
would the edge incorrectly flag a leak? Your hysteresis in mode switching doesn’t explicitly cover the
leak_flag (it’s instantaneous when triggered). Perhaps implement a minimal debounce: require
leak_flag to persist for, say, 2–3 samples (0.06 s) before switching mode, just to avoid flapping if one
sample glitched. It’s a minor tweak, likely not needed in simulation, but worth a mention as future
improvement for real-world robustness.
Data Loss and Backup: In critical systems, one might ask: what if a leak happens but network is
down – the edge will detect it, but then it cannot send data to cloud. In your design, the edge still
never sends raw unless authorized by cloud (i.e., gets the command). If network is down, that
command won’t come. However, the edge could locally log high-frequency data when it detects a
leak, and then send it later when connectivity is restored (or at least send an alarm via any available
channel). This is an edge-case scenario (pun intended) beyond your current implementation. But you
can reason about it: “Even if the cloud link goes down, the edge can still log the event (and perhaps raise
a local alarm). Once connectivity returns, it could upload the buffered data.” This wasn’t implemented
explicitly, but since raw data never leaves unless asked, one could extend the system to handle that
by giving the edge agent more autonomy in emergency. It’s a potential improvement for reliability in
real   deployments.   Universities   and   industry   folks   might   appreciate   that   thought,   as   it   shows
awareness of real-world conditions.
## •
## •
## •
## 16

Benchmark Against Related Work:  As part of your evaluation, you’ve already lined up what to
compare (bandwidth, latency, etc.). You can further bolster your claims by citing numbers from other
works:
For example, if a traditional SCADA system sends everything, how much data is that? You can
calculate: 50 Hz * 4 sensors * (say) 8 bytes each = 1600 bytes/s, ~1.6 kB/s, ~138 MB/day for one
sensor node. Multiply by many nodes – it’s huge. Some industry whitepapers might say “we cannot
stream all sensor data due to cost.” (If you find any reference on typical limits or costs, include it.)
Your 98% reduction means ~2.8 MB/day instead, which is a big difference (if using cellular, that saves
money).
On leak detection, maybe cite that traditionally, leak detection systems often require <1 minute
detection or so, and yours is <1 second which is far beyond requirement. Or cite a case: “Cloud-only
leak detection in a certain study took on the order of seconds or tens of seconds due to data aggregation,
whereas our edge approach is instantaneous.” Actually, [17†L191-L199] describes older methods (visual
inspection, basic pressure monitoring) being slow and real-time systems being needed. You can use
that to emphasize why real-time (sub-second) is important: leaks can have severe consequences
within minutes, so every second counts. By catching leaks in <1 s, you enable faster mitigation
compared to if you noticed it 5–10 s or minutes later.
Visualization Improvements:  If   Grafana   is   proving   too   “confusing”   for   some   comparisons,
remember you can always post-process data with Python (pandas, matplotlib) to create custom
plots. In fact, your script likely generates nicer-looking figures for the thesis. Use Grafana mainly as a
sanity check and a way to capture the system behavior dynamically. Don’t stress it to do everything.
It’s not trivial to, say, overlay three different scenario runs in Grafana without a lot of trickery – it’s
simpler to do that offline.
What Others Do Better / What to Change: To explicitly address “what our university’s other projects
do better and what I should change”:
Other   research   by   Dr.   Aral   and   colleagues   often   incorporate  machine learning and more
autonomous behavior at the edge. For instance, they might deploy a tiny neural network on the
edge device for anomaly detection, rather than a simple threshold, to improve accuracy or adapt to
changing conditions (concept drift). They also explore neuromorphic hardware for ultra-efficient
event processing. In comparison, your project uses a simpler approach (threshold + rule-based
control), but that’s actually fine for a proof-of-concept. The simplicity means it’s transparent and
robust. However, you could mention as future work: integrating an ML model that learns normal
pressure patterns and flags anomalies could reduce false alarms or detect more subtle leaks. The
architecture would support it – just swap out the leak detection module in the edge agent.
Another aspect is evaluation with real data or at scale. Some projects might test on real sensor
data or larger networks. If your simulation is limited, you might suggest validating the system on a
real water pipeline testbed or with more nodes as a next step. The fact that your results are
deterministic and simulation-based is good for scientific proof, but demonstrating it with real noise
and variability would strengthen it further (perhaps outside the scope of a Master’s thesis timeline,
though).
What to change now? Not much in terms of core architecture – it’s solid. Focus on improving how you
demonstrate  it: - Make sure the Grafana dashboards or generated figures clearly illustrate the points
## •
## •
## •
## •
## •
## 17

(some early test plots and adjusting of axes/legends can help). - Double-check synchronization of data
sources (e.g., ensure your InfluxDB and Prometheus times align if you use both, to avoid confusion on
graphs). - Possibly log a bit more info if needed (e.g., ensure you log when mode changes precisely – either
via the decision log or by the edge printing something – so you can correlate events easily during analysis).
By covering these bases, you’ll show that you have not only implemented a working solution but also are
aware  of  how  it  stands  in  context  and  how  it  could  be  enhanced.  The  combination  of  conceptual
soundness, rigorous data, and references to existing research will make your thesis defense strong.
## Conclusion
You have built a comprehensive system that effectively demonstrates the value of a feedback-controlled,
hierarchical   IoT   architecture   for   critical   infrastructure   monitoring.   In   analysis:   -  The   data   and
implementation are sufficient to prove your claims: you’ve instrumented the system to measure exactly
those metrics (bandwidth, latency, CPU) that map to efficiency, responsiveness, and resilience. - Grafana
and visualization:  Make use of the recorded data smartly – highlight the “story” of the feedback loop
(normal → leak occurs → edge detects → cloud commands debug → data spike → leak over → back to
normal; and separately high load → economy mode → etc.). Each of those transitions is evidence that your
adaptive loop works as intended. Cite those moments as proof points. -  Comparison to baseline and
static: Quantify the improvements (how much less data, how much faster detection) and use visual aids
(graphs, tables) to make the comparison easy to grasp. The summary table you have in documentationis a
nice reference – you will now have real data to fill in those qualitative levels with actual numbers.
In sum, the project is in great shape conceptually. The remaining effort is mostly in presenting the results
clearly  and perhaps fine-tuning the simulation or logging for completeness. By learning from similar
research (adaptive sampling saving energy, event-triggered sensing reducing data, edge/cloud coordination
for reliability), you’ve ensured your design is state-of-the-art. And by implementing features like logging,
seeding, and hysteresis, you’ve shown engineering rigor. If you address the minor points (ensuring baseline
detection is fair, using Grafana effectively, considering multi-node or fail-safe behavior conceptually), you
will have a very compelling thesis demonstration.
Don’t be afraid to be detailed in your thesis about these comparisons – you now have both the practical
evidence and the literature support to assert that adaptive edge-cloud feedback can indeed achieve the
“Goldilocks” solution:  near-zero bandwidth use during normal operation, yet no compromise on critical data
during emergencies. This is a significant result with implications for IoT deployments in many domains
(water, energy, etc.), and you should feel confident highlighting that. Good luck, and I look forward to the
figures showing those dramatic spikes and savings – they will tell the story effectively.
## 18