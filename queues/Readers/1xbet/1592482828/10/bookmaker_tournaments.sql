INSERT INTO bookmaker_tournaments (id, fk_bookmaker_id, fk_bookmaker_sport_id, title, skip, mapped, created_at, found_in) VALUES 
(DEFAULT, 14, {sport=Football}, 'FIFA. eSports Battle. Night FA Cup', 0, 0, '2020-06-18 00:00:00', 'West Ham United (MeLToSiK) vs Everton (KRaftVK)')
,(DEFAULT, 14, {sport=Darts}, 'PDC World Matchplay. 2020. Winner', 0, 0, '2020-06-18 00:00:00', 'PDC World Matchplay. 2020. Winner vs ')
,(DEFAULT, 14, {sport=Martial Arts}, 'Combatsport. KSW', 0, 0, '2020-06-18 00:00:00', 'Artur Sowinski vs Gracjan Szadzinski')
,(DEFAULT, 14, {sport=Formula 1}, 'World Constructors´ Championship 2020', 0, 0, '2020-06-18 00:00:00', 'World Constructors´ Championship 2020. Winner vs ')
,(DEFAULT, 14, {sport=Football}, 'FIFA. eSports Battle. Retro International', 0, 0, '2020-06-18 00:00:00', 'England (TAKA) vs France (mooneycb)')
,(DEFAULT, 14, {sport=UFC}, 'UFC Fight Night. 02.08.20', 0, 0, '2020-06-18 00:00:00', 'Markus Perez Echeimberg vs Eric Spicely')
,(DEFAULT, 14, {sport=Tennis}, 'Great Britain. Birmingham. Simulated Reality. Women', 0, 0, '2020-06-18 00:00:00', 'Petra Martic (SRL) vs Iga Swiatek (SRL)')
,(DEFAULT, 14, {sport=Football}, 'Finland. Kakkonen. Division 2', 0, 0, '2020-06-18 00:00:00', 'Hercules Oulu vs Kemi City')
,(DEFAULT, 14, {sport=Tennis}, 'Germany. Berlin. Simulated Reality. Women', 0, 0, '2020-06-18 00:00:00', 'Madison Keys (SRL) vs Daria Gavrilova (SRL)')
,(DEFAULT, 14, {sport=Football}, 'Slovakia. Liga 2', 0, 0, '2020-06-18 00:00:00', 'Poprad vs Dukla Banska Bystrica')
,(DEFAULT, 14, {sport=Football}, 'Finland. Kansallinen League. Women', 0, 0, '2020-06-18 00:00:00', 'TiPS Vantaa (Women) vs Honka (Women)')
,(DEFAULT, 14, {sport=Football}, 'Sweden. Division 2. Norra Svealand', 0, 0, '2020-06-18 00:00:00', 'Gamla Upsala vs Fanna BK')
,(DEFAULT, 14, {sport=Tennis}, 'Germany. Halle. Simulated Reality', 0, 0, '2020-06-18 00:00:00', 'Guido Pella (SRL) vs Matthew Ebden (SRL)')
,(DEFAULT, 14, {sport=Golf}, 'The King & Bear Classic. 2020. Results', 0, 0, '2020-06-18 00:00:00', 'The King & Bear Classic. 2020. Winner vs ')
,(DEFAULT, 14, {sport=Esports}, 'CS:GO. Aorus League. Season 2. Brazil', 0, 0, '2020-06-18 00:00:00', 'SWS Gaming vs Detona Pound')
,(DEFAULT, 14, {sport=Tennis}, 'Eastern European Championship', 0, 0, '2020-06-18 00:00:00', 'Mirza Basic vs Ergi Kirkin')
,(DEFAULT, 14, {sport=Esports}, 'CS:GO. Asia Champions League', 0, 0, '2020-06-18 00:00:00', 'ZIGMA vs Big Time Regal Gaming')
,(DEFAULT, 14, {sport=Esports}, 'CS:GO. Cyber.Bet Summer Cup', 0, 0, '2020-06-18 00:00:00', 'Unicorns Of Love vs SKADE')
,(DEFAULT, 14, {sport=Esports}, 'League of Legends. LPLOL Summer', 0, 0, '2020-06-18 00:00:00', 'Karma Clan Esports vs For The Win')
ON CONFLICT (fk_bookmaker_id, fk_bookmaker_sport_id, title) DO NOTHING;