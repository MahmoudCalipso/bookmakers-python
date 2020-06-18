INSERT INTO bookmaker_tournaments (id, fk_bookmaker_id, fk_bookmaker_sport_id, title, skip, mapped, created_at, found_in) VALUES 
(DEFAULT, 14, {sport=Football}, 'Switzerland. SuperLeague', 0, 0, '2020-06-18 00:00:00', 'Luzern vs Basel')
,(DEFAULT, 14, {sport=Football}, 'England. Championship', 0, 0, '2020-06-18 00:00:00', 'West Bromwich Albion vs Birmingham City')
,(DEFAULT, 14, {sport=Football}, 'Romanian Cup', 0, 0, '2020-06-18 00:00:00', 'Dinamo Bucuresti vs Steaua Bucuresti')
,(DEFAULT, 14, {sport=Football}, 'England. League One', 0, 0, '2020-06-18 00:00:00', 'Portsmouth vs Oxford United')
,(DEFAULT, 14, {sport=Football}, 'Greek Cup', 0, 0, '2020-06-18 00:00:00', 'Aris vs AEK Athens')
,(DEFAULT, 14, {sport=UFC}, 'UFC 253', 0, 0, '2020-06-18 00:00:00', 'Khabib Nurmagomedov vs Justin Gaethje')
,(DEFAULT, 14, {sport=UFC}, 'UFC 252', 0, 0, '2020-06-18 00:00:00', 'Felice Herrig vs Virna Jandiroba')
,(DEFAULT, 14, {sport=UFC}, 'UFC Fight Night. 28.06.20', 0, 0, '2020-06-18 00:00:00', 'Sean Woodson vs Kyle Nelson')
,(DEFAULT, 14, {sport=UFC}, 'UFC Fight Night. 16.07.20', 0, 0, '2020-06-18 00:00:00', 'Molly McCann vs Taila Santos')
ON CONFLICT (fk_bookmaker_id, fk_bookmaker_sport_id, title) DO NOTHING;