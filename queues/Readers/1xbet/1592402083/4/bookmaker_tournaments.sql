INSERT INTO bookmaker_tournaments (id, fk_bookmaker_id, fk_bookmaker_sport_id, title, skip, mapped, created_at, found_in) VALUES 
(DEFAULT, 14, {sport=Volleyball}, 'Vietnam Championship', 0, 0, '2020-06-17 00:00:00', 'Ha Tinh vs Bien Phong')
,(DEFAULT, 14, {sport=Volleyball}, 'Vietnam Championship. Women', 0, 0, '2020-06-17 00:00:00', 'Kinh Bac (Women) vs Thong tin Lien Viet Post Bank (Women)')
,(DEFAULT, 14, {sport=Football}, 'Iceland. 4 Delid', 0, 0, '2020-06-17 00:00:00', 'Hamar Hveragerdi vs KM Reykjavik')
,(DEFAULT, 14, {sport=Esports}, 'League of Legends. OPL Split 2', 0, 0, '2020-06-17 00:00:00', 'Dire Wolves vs Gravitas')
ON CONFLICT (fk_bookmaker_id, fk_bookmaker_sport_id, title) DO NOTHING;