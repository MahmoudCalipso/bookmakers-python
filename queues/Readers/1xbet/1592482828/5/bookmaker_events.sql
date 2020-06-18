INSERT INTO bookmaker_events (id, fk_bookmaker_id, fk_event_id, title, event_id, date) values 
(DEFAULT, 14 {sport=Football&tournament=Denmark Super League&event=Esbjerg  vs Hobro IK&date=2020-06-21 16:00:00}, 'Esbjerg  vs Hobro IK', '241350872', '2020-06-21 16:00:00')
,(DEFAULT, 14 {sport=Football&tournament=Norway Eliteserien&event=Haugesund vs Brann&date=2020-06-17 20:30:00}, 'Haugesund vs Brann', '236910224', '2020-06-17 20:30:00')
,(DEFAULT, 14 {sport=Football&tournament=Bundesliga 2&event=Darmstadt 98 vs Wehen Wiesbaden&date=2020-06-21 15:30:00}, 'Darmstadt 98 vs Wehen Wiesbaden', '241203729', '2020-06-21 15:30:00')
,(DEFAULT, 14 {sport=Football&tournament=Denmark Super League&event=Brondby vs København&date=2020-06-21 18:00:00}, 'Brondby vs København', '241353028', '2020-06-21 18:00:00')
,(DEFAULT, 14 {sport=Football&tournament=Bundesliga 2&event=Bochum vs Greuther Fürth&date=2020-06-21 15:30:00}, 'Bochum vs Greuther Fürth', '241203731', '2020-06-21 15:30:00')
,(DEFAULT, 14 {sport=Football&tournament=Denmark Super League&event=AC Horsens vs Randers&date=2020-06-20 13:00:00}, 'AC Horsens vs Randers', '241348051', '2020-06-20 13:00:00')
,(DEFAULT, 14 {sport=Football&tournament=Denmark Super League&event=Randers vs Hobro IK&date=2020-06-16 20:00:00}, 'Randers vs Hobro IK', '240685289', '2020-06-16 20:00:00')
,(DEFAULT, 14 {sport=Football&tournament=Norway Eliteserien&event=Viking vs Bodø/Glimt&date=2020-06-16 18:00:00}, 'Viking vs Bodø/Glimt', '236909460', '2020-06-16 18:00:00')
,(DEFAULT, 14 {sport=Football&tournament=Denmark Super League&event=AC Horsens vs Esbjerg &date=2020-06-16 18:00:00}, 'AC Horsens vs Esbjerg ', '240684407', '2020-06-16 18:00:00')
,(DEFAULT, 14 {sport=Football&tournament=Norway Eliteserien&event=Kristiansund vs Aalesund&date=2020-06-20 17:00:00}, 'Kristiansund vs Aalesund', '240558448', '2020-06-21 18:00:00')
,(DEFAULT, 14 {sport=Football&tournament=Norway Eliteserien&event=Rosenborg vs Kristiansund&date=2020-06-16 18:00:00}, 'Rosenborg vs Kristiansund', '236909212', '2020-06-16 18:00:00')
,(DEFAULT, 14 {sport=Football&tournament=Norway Eliteserien&event=Aalesund vs Molde&date=2020-06-16 18:00:00}, 'Aalesund vs Molde', '236908761', '2020-06-16 18:00:00')
,(DEFAULT, 14 {sport=Football&tournament=Norway Eliteserien&event=Bodø/Glimt vs Haugesund&date=2020-06-20 17:00:00}, 'Bodø/Glimt vs Haugesund', '240557984', '2020-06-21 18:00:00')
,(DEFAULT, 14 {sport=Football&tournament=Norway Eliteserien&event=Molde vs Rosenborg&date=2020-06-20 20:30:00}, 'Molde vs Rosenborg', '240557519', '2020-06-20 20:30:00')
,(DEFAULT, 14 {sport=Football&tournament=Norway Eliteserien&event=Sandefjord vs Start&date=2020-06-21 18:00:00}, 'Sandefjord vs Start', '240557134', '2020-06-21 18:00:00')
ON CONFLICT (fk_bookmaker_id, fk_event_id) DO NOTHING;