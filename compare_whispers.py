#!/usr/bin/env hpc_python.sif

import argparse
import jiwer
from pathlib import Path
import logging
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on debugging")
    parser.add_argument('base', type=Path, help='Base whisper')
    parser.add_argument('comp', type=Path, help="Thing to compare to base")
    parser.add_argument('output', type=Path, help='Output directory')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [%(process)d:%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")
    
    todo = []
    if args.base.is_file():
        if not args.comp.is_file():
            logging.error("If base is a file then comp must also be a file")
            exit(1)
        logging.debug(f"Adding {args.base}, {args.comp}")
        todo.append([args.base, args.comp])
    elif args.base.is_dir():
        if not args.comp.is_dir():
            logging.error("If base is a directory then comp must also be a directory")
            exit(1)
        for f in args.base.glob("**/*.json"):            
            rf = f.relative_to(args.base)
            cf = (args.comp / rf)
            if not cf.exists():                
                logging.warning(f"Skipping base file {rf} as there isn't a corresponding file in {args.comp}")
                continue
            todo.append([f, cf])
    else:
        print(f"{args.base} is neither file nor dir? {args.base.stat()}")

    if not args.output.is_dir():
        is_file = True
        outfile = open(args.output, "w")
    else:
        is_file = False
    print(todo)
    for b, c in todo:
        with open(b) as f:
            bdata = json.load(f)
        with open(c) as f:
            cdata = json.load(f)
        
        o = jiwer.process_words(bdata['text'], cdata['text'])
        v, stats = generate_visualization(o)

        report = []

        report.extend([
            f"Base File: {b.resolve()!s}",
        ])
        if '_job' in bdata:
            report.extend([
                f"  Media duration:    {s2time(bdata['_job']['media_duration'])}",
                f"  Processing time:   {s2time(bdata['_job']['runtime'])}"                
            ])
            content_time = bdata['faster_whisper_info'][3] if 'faster_whisper_info' in bdata else bdata['_job']['media_duration']
            report.extend([
                f"  Content duration:  {s2time(content_time)}",
                f"  Processing rate:   {content_time / bdata['_job']['runtime']:0.3f} content seconds per clock second"
            ])

        report.extend([
            f"Comp File: {c.resolve()!s}"
        ])
        if '_job' in cdata:
            report.extend([
                f"  Media duration:    {s2time(cdata['_job']['media_duration'])}",
                f"  Processing time:   {s2time(cdata['_job']['runtime'])}"                
            ])
            content_time = cdata['faster_whisper_info'][3] if 'faster_whisper_info' in cdata else cdata['_job']['media_duration']
            report.extend([
                f"  Content duration:  {s2time(content_time)}",
                f"  Processing rate:   {content_time / cdata['_job']['runtime']:0.3f} content seconds per clock second"
            ])            
        
        report.append("")

        report.extend([
            "Stats: ",
            f"  Word Error Rate:            {o.wer * 100:7.2f}%",
            f"  Word Information Lost:      {o.wil * 100:7.2f}%",
            f"  Word Information Preserved: {o.wip * 100:7.2f}%",
            f"  Match Error Rate:           {o.mer * 100:7.2f}%",
            "",
            "Edit Stats:",
            f"  Hits:          {stats['hit']:>5d}",
            f"  Inserts:       {stats['ins']:>5d}",
            f"  Deletes:       {stats['del']:>5d}",
            f"  Substitutions: {stats['sub']:>5d}",
            "",
            "Edits:"])
        for s in v:            
            report.append(f"BASE: {s['ref']}")
            report.append(f"COMP: {s['hyp']}")
            report.append(f"EDIT: {s['chg']}")
            report.append("")
        
        if is_file:
            report.append("-----------")
            outfile.write("\n".join(report) + "\n")
        else:
            with open(args.output / b.name, "w") as outfile:
                outfile.write("\n".join(report))

    if is_file:
        outfile.close()

def pad(word, pad_len):
    while len(word) < pad_len:
        word += " "
    return word

def generate_visualization(output: jiwer.WordOutput, length=75):
    results = [{'ref': '', 'hyp': '', 'chg': ''}]
    stats = {'hit': 0, 'sub': 0, 'del': 0, 'ins': 0}
    for idx, (gt, hp, chunks) in enumerate(zip(output.references, output.hypotheses, output.alignments)):
        #print(idx, gt, hp, chunks)
        for chunk in chunks:
            if chunk.type == 'equal':
                # copy ref, and hyp words until either we
                # end up too long or we come to the end.                    
                for i in range(chunk.ref_end_idx - chunk.ref_start_idx):                
                    stats['hit'] += 1
                    word_len = len(gt[i + chunk.ref_start_idx]) 
                    if word_len + len(results[-1]['ref']) + 1> length:
                        # too long. create a new result
                        results.append({'ref': '', 'hyp': '', 'chg': ''})
            
                    results[-1]['ref'] += gt[i + chunk.ref_start_idx] + " "
                    results[-1]['hyp'] += hp[i + chunk.hyp_start_idx] + " "
                    results[-1]['chg'] += ' ' * (word_len + 1)     
            elif chunk.type == 'insert':
                # hyp has an additional word that's not in ref.                
                for i in range(chunk.hyp_end_idx - chunk.hyp_start_idx):                
                    stats['ins'] += 1
                    word_len = len(hp[i + chunk.hyp_start_idx])
                    if word_len + len(results[-1]['ref']) + 1> length:
                        # too long. create a new result
                        results.append({'ref': '', 'hyp': '', 'chg': ''})
            
                    results[-1]['ref'] += ('*' * word_len) + " "
                    results[-1]['hyp'] += hp[i + chunk.hyp_start_idx] + " "
                    results[-1]['chg'] += ('I' * word_len) + " "
            elif chunk.type == 'delete':
                # ref has an additional word that's not in hyp.                
                for i in range(chunk.ref_end_idx - chunk.ref_start_idx): 
                    stats['del'] += 1               
                    word_len = len(gt[i + chunk.ref_start_idx])
                    if word_len + len(results[-1]['ref']) + 1> length:
                        # too long. create a new result
                        results.append({'ref': '', 'hyp': '', 'chg': ''})
            
                    results[-1]['ref'] += gt[i + chunk.ref_start_idx] + " "
                    results[-1]['hyp'] += ('*' * word_len) + " "                    
                    results[-1]['chg'] += ('D' * word_len) + " "
            elif chunk.type == 'substitute':
                # ref and hyp have different words (but the same number)                
                for i in range(chunk.ref_end_idx - chunk.ref_start_idx):
                    stats['sub'] += 1                
                    word_len = max([len(gt[i + chunk.ref_start_idx]),
                                    len(hp[i + chunk.hyp_start_idx])])
                    if word_len + len(results[-1]['ref']) + 1> length:
                        # too long. create a new result
                        results.append({'ref': '', 'hyp': '', 'chg': ''})
            
                    results[-1]['ref'] += pad(gt[i + chunk.ref_start_idx], word_len) + " "
                    results[-1]['hyp'] += pad(hp[i + chunk.hyp_start_idx], word_len) + " "
                    results[-1]['chg'] += 'S' * (word_len) + ' '
            else:
                print(chunk)

    return results, stats


def s2time(seconds):        
    hours = int(seconds / 3600)
    seconds -= hours * 3600
    minutes = int(seconds / 60)
    seconds -= minutes * 60
    return f"{hours:0d}:{minutes:02d}:{seconds:06.3f}"


if __name__ == "__main__":
    main()