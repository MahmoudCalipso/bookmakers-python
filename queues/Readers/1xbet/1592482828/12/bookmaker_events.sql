INSERT INTO bookmaker_events (id, fk_bookmaker_id, fk_event_id, title, event_id, date) values 
(DEFAULT, 14 {sport=Baseball&tournament=Virtual MLB&event=Texas Rangers vs Colorado Rockies&date=2020-06-17 00:00:00}, 'Texas Rangers vs Colorado Rockies', '241152536', '2020-06-17 00:00:00')
,(DEFAULT, 14 {sport=Baseball&tournament=Virtual MLB&event=Cincinnati Reds vs Kansas City Royals&date=2020-06-17 00:00:00}, 'Cincinnati Reds vs Kansas City Royals', '241152757', '2020-06-17 00:00:00')
,(DEFAULT, 14 {sport=Baseball&tournament=Virtual MLB&event=Chicago White Sox vs San Diego Padres&date=2020-06-17 01:30:00}, 'Chicago White Sox vs San Diego Padres', '241153311', '2020-06-17 01:30:00')
,(DEFAULT, 14 {sport=Baseball&tournament=Virtual MLB&event=Colorado Rockies vs Baltimore Orioles&date=2020-06-17 01:30:00}, 'Colorado Rockies vs Baltimore Orioles', '241153312', '2020-06-17 01:30:00')
,(DEFAULT, 14 {sport=Baseball&tournament=Virtual MLB&event=Kansas City Royals vs Chicago Cubs&date=2020-06-17 03:00:00}, 'Kansas City Royals vs Chicago Cubs', '241153522', '2020-06-17 03:00:00')
,(DEFAULT, 14 {sport=Baseball&tournament=Virtual MLB&event=San Diego Padres vs Detroit Tigers&date=2020-06-17 03:00:00}, 'San Diego Padres vs Detroit Tigers', '241153523', '2020-06-17 03:00:00')
,(DEFAULT, 14 {sport=Baseball&tournament=Virtual MLB&event=Seattle Mariners vs Philadelphia Phillies&date=2020-06-17 04:30:00}, 'Seattle Mariners vs Philadelphia Phillies', '241153751', '2020-06-17 04:30:00')
,(DEFAULT, 14 {sport=Baseball&tournament=Virtual MLB&event=Oakland Athletics vs Cincinnati Reds&date=2020-06-16 15:00:00}, 'Oakland Athletics vs Cincinnati Reds', '241177885', '2020-06-16 15:00:00')
,(DEFAULT, 14 {sport=Baseball&tournament=Virtual MLB&event=Pittsburgh Pirates vs Chicago White Sox&date=2020-06-17 04:30:00}, 'Pittsburgh Pirates vs Chicago White Sox', '241153943', '2020-06-17 04:30:00')
,(DEFAULT, 14 {sport=Baseball&tournament=Virtual MLB&event=Toronto Blue Jays vs San Francisco Giants&date=2020-06-17 06:00:00}, 'Toronto Blue Jays vs San Francisco Giants', '241154177', '2020-06-17 06:00:00')
,(DEFAULT, 14 {sport=Baseball&tournament=Virtual MLB&event=Miami Marlins vs Toronto Blue Jays&date=2020-06-17 06:00:00}, 'Miami Marlins vs Toronto Blue Jays', '241154391', '2020-06-17 06:00:00')
,(DEFAULT, 14 {sport=Baseball&tournament=Virtual MLB&event=Boston Red Sox vs Los Angeles Dodgers&date=2020-06-16 16:30:00}, 'Boston Red Sox vs Los Angeles Dodgers', '241150767', '2020-06-16 16:30:00')
,(DEFAULT, 14 {sport=Table Tennis&tournament=Challenger Series Men&event=BLUHM Florian vs CHEAIB Dauud&date=2020-06-17 09:50:00}, 'BLUHM Florian vs CHEAIB Dauud', '241441440', '2020-06-17 09:50:00')
,(DEFAULT, 14 {sport=Table Tennis&tournament=Russia Liga Pro&event=Vitaly Bazilevsky vs Yuri Merkushin&date=2020-06-16 12:00:00}, 'Vitaly Bazilevsky vs Yuri Merkushin', '241404212', '2020-06-16 12:00:00')
,(DEFAULT, 14 {sport=E-Sports&tournament=Orange Unity League&event=sAw vs KPI Gaming&date=2020-06-16 20:00:00}, 'sAw vs KPI Gaming', '241274109', '2020-06-16 20:00:00')
,(DEFAULT, 14 {sport=E-Sports&tournament=Orange Unity League&event=Team Heretics vs S2V Esports&date=2020-06-17 20:00:00}, 'Team Heretics vs S2V Esports', '241288244', '2020-06-17 20:00:00')
,(DEFAULT, 14 {sport=Table Tennis&tournament=Challenger Series Men&event=BLUHM Florian vs QIU Liang&date=2020-06-17 13:30:00}, 'BLUHM Florian vs QIU Liang', '241364268', '2020-06-17 13:30:00')
,(DEFAULT, 14 {sport=Table Tennis&tournament=Challenger Series Men&event=QIU Liang vs CHEAIB Dauud&date=2020-06-17 12:30:00}, 'QIU Liang vs CHEAIB Dauud', '241364265', '2020-06-17 12:30:00')
ON CONFLICT (fk_bookmaker_id, fk_event_id) DO NOTHING;