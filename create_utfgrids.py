#!/usr/bin/env python
# -*- coding: utf-8  -*-
"""
create_utfgrids.py
Author: Matthew Perry
License: BSD

Creates utfgrid .json tiles for the given polygon shapefile

Thx to Dane Springmeyer for the utfgrid spec and mapnik rendering code
and to  Klokan Petr Přidal for his MapTiler code 
(http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/)
"""
import globalmaptiles
import mapnik
import ogr
import os
from optparse import OptionParser, OptionError
try:
    import simplejson as json
except ImportError:
    import json

import multiprocessing

# number of processes
numProcesses = 10

def create_utfgrids(shppath, minzoom, maxzoom, outdir, fields=None, layernum=0):
    ds = ogr.Open(shppath)
    print 
    print "WARNING:"
    print " This script assumes a polygon shapefile in spherical mercator projection."
    print " If any of these assumptions are not true, don't count on the results!"
    # TODO confirm polygons
    # TODO confirm mercator
    # TODO get layernum from command line 
    layer = ds.GetLayer(layernum)
    bbox = layer.GetExtent()

    mercator = globalmaptiles.GlobalMercator()
            
    m = mapnik.Map(256,256)

    # Since grids are `rendered` they need a style 
    s = mapnik.Style()
    r = mapnik.Rule()
    polygon_symbolizer = mapnik.PolygonSymbolizer()
    r.symbols.append(polygon_symbolizer)
    line_symbolizer = mapnik.LineSymbolizer()
    r.symbols.append(line_symbolizer)
    s.rules.append(r)
    m.append_style('My Style',s)

    ds = mapnik.Shapefile(file=shppath)
    mlayer = mapnik.Layer('poly')
    mlayer.datasource = ds
    mlayer.styles.append('My Style')
    m.layers.append(mlayer)

    if fields is None:
        fields = mlayer.datasource.fields() 

    for tz in range(minzoom, maxzoom+1):
        print " * Processing Zoom Level %s" % tz
        tminx, tminy = mercator.MetersToTile( bbox[0], bbox[2], tz)
        tmaxx, tmaxy = mercator.MetersToTile( bbox[1], bbox[3], tz)

        def writeUTFGRID(q):
            while not q.empty():
                r = q.get()
                if (r == None):
                    q.task_done()
                    break
                else:
                    (ty, tx, tz) = r

                # print (tz, tx, ty)
                output = os.path.join(outdir, str(tz), str(tx))
                if not os.path.exists(output):
                    os.makedirs(output)

                # Use top origin tile scheme (like OSM or GMaps)
                # TODO support option for TMS bottom origin scheme (ie opt to not invert)
                ymax = 1 << tz;
                invert_ty = ymax - ty - 1;

                tilefilename = os.path.join(output, "%s.json" % invert_ty) # ty for TMS bottom origin
                tilebounds = mercator.TileBounds( tx, ty, tz)
                #print tilefilename, tilebounds

                box = mapnik.Box2d(*tilebounds)
                m.zoom_to_box(box)
                grid = mapnik.Grid(m.width,m.height)
                mapnik.render_layer(m,grid,layer=0,fields=fields)
                utfgrid = grid.encode('utf',resolution=4)
                with open(tilefilename, 'w') as file:
                    file.write(json.dumps(utfgrid))

                q.task_done()

        # queue
        q = multiprocessing.JoinableQueue()

        for ty in range(tminy, tmaxy+1):
            for tx in range(tminx, tmaxx+1):
                q.put((ty, tx, tz))

        # processes
        processes = []
        for i in range(numProcesses):
            p = multiprocessing.Process(target=writeUTFGRID, args=(q,))
            processes.append(p)
            p.start()

        for i in range(numProcesses):
            q.put(None)
        q.join()
        for i in range(numProcesses):
            processes[i].join()

if __name__ == "__main__":
    usage = "usage: %prog [options] shapefile minzoom maxzoom output_directory"
    parser = OptionParser(usage)
    parser.add_option("-f", '--fields', dest="fields", help="Comma-seperated list of fields; default is all")
    (options, args) = parser.parse_args()

    if len(args) != 4:
        parser.error("Incorrect number of arguments")
        
    shppath = args[0]
    minzoom, maxzoom = int(args[1]), int(args[2])
    outdir = args[3]

    if os.path.exists(outdir):
        parser.error("output directory exists already")

    if options.fields:
        fields = options.fields.split(",")
    else:
        fields = None

    create_utfgrids(shppath, minzoom, maxzoom, outdir, fields)
