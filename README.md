# DRT Analytics App

## Introduction
This app produces ML models for arrivals:
 - How many minutes off its scheduled arrival time an arrival will be
 - How many minutes an arrival will take to get to chox from touchdown
 - The likely walk time passengers will have from the plane to the immigration hall

## Accuracy

Various models were tested for accuracy against LHR T2 data

6 months of data were used - 5 for training and the most recent 1 month for validation

### Off-schedule minutes

The best combination of model partition & features was found to be

Terminal / Origin :: day / am/pm / carrier / flight number

Percentages in the headings refer to improvement in accuracy over the baseline model

Percentages in the table refer numbers of arrivals with the given improvement, eg 35% of arrivals had an improvement of 10% or more

| Partition                     | Features                                          | 10% | 20% | 30% | 40% | 50% | 60% | 70% | 80% | 90% | 100% |
|-------------------------------|---------------------------------------------------|-----|-----|-----|-----|-----|-----|-----|-----|-----|------|
| Term / Origin                 | day of week / am or pm / carrier / flight number  | 34% | 23% | 18% | 12% | 5%  | 5%  | 5%  | -   | -   | -    |
| Term / Flight Number / Origin | day of week / am or pm                            | 31% | 23% | 18% | 12% | 7%  | 5%  | 5%  | -   | -   | -    |
| Term / Carrier                | day of week / am or pm / origin / flight number   | 23% | 17% | 11% | 8%  | 2%  | 2%  | 2%  | -   | -   | -    |


### Chox minutes

The best combination of model partition & features was found to be

Terminal / Carrier :: day / am/pm / origin

| Partition                     | Features                                        | 10% | 20% | 30% | 40% | 50% | 60% | 70% | 80% | 90% | 100% |
|-------------------------------|-------------------------------------------------|-----|-----|-----|-----|-----|-----|-----|-----|-----|------|
| Term / Carrier                | day of week / am or pm / origin                 | 88% | 73% | 47% | 29% | 17% | 8%  | 5%  | 2%  | 2%  | 2%   |
| Term / Carrier                | day of week / am or pm / origin / flight number | 88% | 70% | 47% | 29% | 20% | 8%  | 5%  | 2%  | 2%  | 2%   |
| Term / Flight Number / Origin | day of week / am or pm                          | 66% | 44% | 26% | 15% | 8%  | 6%  | 5%  | 4%  | 4%  | 4%   |


### Walk time minutes

The best combination of model partition & features was found to be

Terminal / Carrier :: day of week / am/pm / origin / flight number

| Partition          | Features                                            | 10% | 20% | 30% | 40% | 50% | 60% | 70% | 80% | 90% | 100% |
|--------------------|-----------------------------------------------------|-----|-----|-----|-----|-----|-----|-----|-----|-----|------|
| Terminal / Carrier | day of week / am/pm / origin / flight number        | 91% | 91% | 91% | 91% | 91% | 88% | 73% | 58% | 38% | 14%  |
 | Terminal / Carrier | day of week / am/pm / origin                        | 91% | 91% | 91% | 91% | 91% | 82% | 70% | 52% | 38% | 14%  |
 | Terminal / Origin  | day of week + part of day + carrier + flight number | 83% | 83% | 81% | 81% | 81% | 80% | 70% | 51% | 26% | 9%   |

