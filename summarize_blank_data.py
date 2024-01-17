#!/usr/bin/env python3

import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("src", nargs='+', help="Source (file or directory tree)")
    parser.add_argument("--duration", type=int, default=600, help="Minimum segment duration")
    args = parser.parse_args()

    sources = []
    for s in args.src:        
        source = Path(s)
        if source.is_file():
            sources.append(source)
        else:
            sources.extend(source.glob("**/*.json"))


    results = {}

    for jfile in sources:
        with open(jfile) as f:
            data = json.load(f)
        mdata = data[0]
        mtype = ('A' if mdata['has_audio'] else '') + ('V' if mdata['has_video'] else '')
        if mtype not in results:
            results[mtype] = {'total': 0, 'airy': 0, 'duration': 0, 'beginning': 0, 'middle': 0, 'end': 0, 'empty': 0, 'tail': 0}
        results[mtype]['total'] += 1
        results[mtype]['duration'] += mdata['end'] - mdata['start']
                
        air = determine_air(data, args.duration)
        if air:
            print(jfile, air)
            results[mtype]['airy'] += 1
            results[mtype]['beginning'] += air['beginning']
            results[mtype]['middle'] += air['middle']
            results[mtype]['end'] += air['end']
            results[mtype]['tail'] += air['tail']
            if air['empty']:
                results[mtype]['empty'] += 1

    for k in results:
        print(k)
        for k2 in results[k]:
            if k2 in ('total', 'airy', 'empty'):
                print(f"  {k2:10s}:  {results[k][k2]:>16}")
            elif k2 == 'duration':
                print(f"  {k2:10s}:  {sec2time(results[k][k2]):>16}")
            else:
                pct = 100 * (results[k][k2] / results[k]['duration'])
                print(f"  {k2:10s}:  {sec2time(results[k][k2]):>16}  ({pct:6.2f}%)")


    #print(results)



def determine_air(data, duration=600):
    metadata = data.pop(0)
    segs = sorted(data, key=lambda x: x['start'])
    ssegs = [x for x in segs if x['type'] == 'silence']
    bsegs = [x for x in segs if x['type'] == 'black']

    # OK, we're going to to something..fun?  There are times
    # where silence is broken by a pop or something that's
    # less than a second long and creates a separate segment.  We're going
    # to join these segments because *POP* isn't content when it's sitting
    # between two greater-than-ten-minute silences
    if ssegs:
        p = 0
        while p < len(ssegs) - 1:
            if ssegs[p + 1]['start'] - ssegs[p]['end'] < 1:
                ssegs[p]['end'] = ssegs[p + 1]['end']
                ssegs.pop(p + 1)
                continue
            p += 1
    # same for black segments -- flashes don't count.
    if bsegs:
        p = 0
        while p < len(bsegs) - 1:
            if bsegs[p + 1]['start'] - bsegs[p]['end'] < 1:
                bsegs[p]['end'] = bsegs[p + 1]['end']
                bsegs.pop(p + 1)
                continue
            p += 1


    results = {'beginning': 0, 'middle': 0, 'end': 0, 'empty': False, 'tail': 0}

    
    # let's filter out the defintely nots...
    # first quick filter:  no segments?  no air!
    if len(segs) == 0:
        return None

    # Second filter: has_video but no black segments?  no air!
    if metadata['has_video'] and len(bsegs) == 0:
        # well, sort of.  If there's a silence segment that is anchored
        # to the end of the video but isn't the whole video, then there's
        # a likelyhood that it's air, but not purely a black screen (i.e. it's
        # static or some jumpy black screen, or blue signal or something)  It's
        # not a sure thing, but I do want to notate it.
        if abs(ssegs[-1]['end'] - metadata['end']) < 1 and ssegs[-1]['end'] - ssegs[-1]['start'] > duration:
            results['tail'] = ssegs[-1]['end'] - ssegs[-1]['start']
            return results

        return None
    
    # Third filter: has_audio but no silence segments?  no air!
    if metadata['has_audio'] and len(ssegs) == 0:
        return None
    
    if metadata['has_audio'] and metadata['has_video']:
        # we have to merge the segments.  If the segments overlap then
        tsegs = []
        for s in ssegs:
            for b in bsegs:
                #print("comparing", s, b)
                if b['start'] <= s['start'] <= b['end']:
                    # start point overlaps
                    tsegs.append({'type': 'silence+black', 
                                  'start': s['start'],
                                  'end': min(s['end'], b['end'])})
                elif b['start'] <= s['end'] <= b['end']:
                    # end point overlaps
                    tsegs.append({'type': 'silence+black',
                                  'start': max(b['start'], s['start']),
                                  'end': s['end']})


    elif metadata['has_audio']:
        tsegs = ssegs
    elif metadata['has_video']:
        tsegs = bsegs
    #print(tsegs)
    # remove an segments that are less than duration seconds.
    tsegs = [x for x in tsegs if x['end'] - x['start'] > duration]

    # if there aren't any, then we've got no appreciable air.
    if len(tsegs) == 0:
        return None

    #print(tsegs)


    # ok, now that we have the test segments, we want to look at three things:
    # air at the beginning, middle, and end.
    
    for t in tsegs:
        if t['start'] <= 1 and abs(t['end'] - metadata['end']) <=1:
            # the whole media?
            results['empty'] = True
        elif t['start'] <= 1:
            # this is one that's (practically) at the beginning
            results['beginning'] += t['end'] - t['start']
        elif abs(t['end'] - metadata['end']) <= 1:
            # practically at the end!
            results['end'] += t['end'] - t['start']
        else:
            results['middle'] += t['end'] - t['start']





    return results


def sec2time(s):
    hours = int(s / 3600)
    s -= hours * 3600
    mins = int(s / 60)
    s -= mins * 60
    return f"{hours:02d}:{mins:02d}:{s:06.3f}"




if __name__ == "__main__":
    main()