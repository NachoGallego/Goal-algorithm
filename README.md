# Goal-algorithm

This project aims to predict the number of goals (+4/-1) in football games of the main european leagues.

# Little context

This google colab scripts use the Poisson distribution to calculate the probabilities of teams scoring agains each other for each matchday.This probabilities are used to feed a Decission tree regressor in order to get the number of goals in each game. Two trees are being used as one is trained with the probabilities of all the leagues games combined and the other one only uses its own league games probabilities. This is simply a way to have a little more information as its hard to get reliable data when only data from the actual season can be used.

# API

This scripts use api.football-data.org to retrieve all game data information.

# Work in progess

I'll be updating the scripts for this season and, also, I'll try to make them cleaner, this is the raw version I use on my own Google colab version so it's a bit of a mess. 

