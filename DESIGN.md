---
name: Project Nexus - Gaming HUD
colors:
  surface: '#0c150f'
  surface-dim: '#0c150f'
  surface-bright: '#323c34'
  surface-container-lowest: '#07100a'
  surface-container-low: '#141e17'
  surface-container: '#18221b'
  surface-container-high: '#232c25'
  surface-container-highest: '#2d3730'
  on-surface: '#dae5da'
  on-surface-variant: '#b9cbbc'
  inverse-surface: '#dae5da'
  inverse-on-surface: '#29332b'
  outline: '#849587'
  outline-variant: '#3b4a3f'
  surface-tint: '#00e38b'
  primary: '#f4fff3'
  on-primary: '#00391f'
  primary-container: '#00ff9d'
  on-primary-container: '#007143'
  inverse-primary: '#006d40'
  secondary: '#c9bfff'
  on-secondary: '#2e009c'
  secondary-container: '#4720ca'
  on-secondary-container: '#baaeff'
  tertiary: '#fffaff'
  on-tertiary: '#3b2f00'
  tertiary-container: '#ffdd65'
  on-tertiary-container: '#766000'
  error: '#ffb4ab'
  on-error: '#690005'
  error-container: '#93000a'
  on-error-container: '#ffdad6'
  primary-fixed: '#56ffa8'
  primary-fixed-dim: '#00e38b'
  on-primary-fixed: '#002110'
  on-primary-fixed-variant: '#00522f'
  secondary-fixed: '#e5deff'
  secondary-fixed-dim: '#c9bfff'
  on-secondary-fixed: '#1a0063'
  on-secondary-fixed-variant: '#441cc8'
  tertiary-fixed: '#ffe17a'
  tertiary-fixed-dim: '#e4c44f'
  on-tertiary-fixed: '#231b00'
  on-tertiary-fixed-variant: '#554500'
  background: '#0c150f'
  on-background: '#dae5da'
  surface-variant: '#2d3730'
  bg-void: '#0B0E14'
  surface-slate: '#151A22'
  border-muted: '#2D333B'
  text-primary: '#FFFFFF'
  text-secondary: '#8B949E'
  critical-red: '#FF3366'
typography:
  display-header:
    fontFamily: Inter
    fontSize: 1.5rem
    fontWeight: '700'
    lineHeight: 2rem
    letterSpacing: 0.05em
  section-title:
    fontFamily: Inter
    fontSize: 0.875rem
    fontWeight: '700'
    lineHeight: 1.25rem
    letterSpacing: 0.1em
  body-md:
    fontFamily: Inter
    fontSize: 1rem
    fontWeight: '400'
    lineHeight: 1.5rem
  body-sm:
    fontFamily: Inter
    fontSize: 0.875rem
    fontWeight: '400'
    lineHeight: 1.25rem
  metric-lg:
    fontFamily: JetBrains Mono
    fontSize: 2.25rem
    fontWeight: '700'
    lineHeight: 2.5rem
  metric-md:
    fontFamily: JetBrains Mono
    fontSize: 1.25rem
    fontWeight: '500'
    lineHeight: 1.75rem
  terminal-log:
    fontFamily: JetBrains Mono
    fontSize: 0.8125rem
    fontWeight: '400'
    lineHeight: 1.2rem
rounded:
  sm: 0.25rem
  DEFAULT: 0.5rem
  md: 0.75rem
  lg: 1rem
  xl: 1.5rem
  full: 9999px
spacing:
  container-padding: 2rem
  gutter: 1.5rem
  stack-sm: 0.5rem
  stack-md: 1rem
  stack-lg: 2rem
---

# Design System: Project Nexus - Real-Time Gaming HUD

## Overview
A dark-themed analytics dashboard for a real-time game recommendation engine. The UI should feel like a modern, high-tech gaming HUD—clean, data-heavy, and emphasizing real-time streaming information.

## Colors
- Primary Background: #0B0E14 (Deep Void Black)
- Surface/Card Background: #151A22 (Dark Slate)
- Primary Accent: #00FF9D (Neon Mint / Active Status)
- Secondary Accent: #7B61FF (Cyber Purple)
- Alert/Warning: #FF3366 (Critical Red)
- Text Primary: #FFFFFF
- Text Secondary: #8B949E

## Typography
- Headers: Clean, sans-serif (e.g., Inter), bold, uppercase for section titles.
- Body: Highly legible sans-serif for data tables.
- Metrics/Numbers: Monospace font (e.g., Roboto Mono) for real-time streaming metrics to give a "terminal" feel.

## Screen Layout & Components
Generate a single-page Desktop Dashboard with the following layout:

1. **Top Header Bar**: 
   - Left: Dashboard Title ("Real-Time ALS Recommender").
   - Right: A pulsing green "Kafka Stream: Live" status indicator.

2. **Top Row (Streaming Metrics)**:
   - Three horizontal data cards showing live network stats: "Throughput (Events/sec)", "Pipeline Latency (<5s)", and "Active Window Users".

3. **Middle Section (Split 50/50)**:
   - **Left Column (Trending Items)**: A ranked leaderboard showing the Top 5 trending games based on the 30-second sliding window. Include a small "+/-" trend indicator next to each game.
   - **Right Column (Personalized Recommendations)**: A highlighted card showing "Top 5 Recommended for User #8472", combining historical batch data and live stream updates.

4. **Bottom Row (Alert System)**:
   - A wide, terminal-style feed displaying a running log of system alerts.
   - Example entries: "[WARNING] Item 120 rating spike > 4.5", "[ALERT] Sudden user activity spike detected". Use the Critical Red color for the alert tags.

## Component Styles
- Cards: Slightly rounded corners (8px), a subtle 1px border (#2D333B), and a faint glowing drop shadow.
- Data visualizers: Use simple, clean bar charts or line graphs if rendering charts.