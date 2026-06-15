"""Night Sky Viewer (planetarium) — scientifically accurate all-sky rendering.

Modules:
  colors    star colour from temperature / colour index (blackbody)
  geometry  Alt/Az computation + dome (stereographic) projection
  catalog   load the Gaia visible-star catalogue + name/line/Milky-Way overlays
  ephem     Sun, Moon, planets and twilight via astropy
  render    build the Plotly dome figure
"""
