import os
import re
import glob
import shutil
import argparse

from osgeo import gdal
from gsclient import GsClient


def moveToCloud( pathname, args ):

    """
    move cog to bucket storage
    """

    url = None
    try:

        # parse uri
        bucket, prefix = GsClient.parseUri( args.out_path )
        if bucket is not None:

            # update credentials
            if os.path.exists( args.key_pathname ):
                GsClient.updateCredentials( args.key_pathname )

            # open client and upload file
            client = GsClient( bucket, chunk_size=args.chunk_size )
            url = client.uploadFile(    pathname, 
                                        os.path.join( prefix, getDateTimeString( pathname ) ),
                                        flatten=True )

    except BaseException as error:
        print ( 'Error: {} '.format ( error ) )


    return url


def getDateTimeString( pathname ):

    """
    parse date time sub-folder name from pathname
    """

    dt = None

    # parse for date time sub directory
    m = re.search( '[0-9]{8}_[0-9]{6}', pathname )
    if m:
        dt = str(m.group(0) )

    return dt


def convertToCog( pathname, args, creationOptions=[ 'BIGTIFF=YES', 'COMPRESS=DEFLATE', 'NUM_THREADS=ALL_CPUS' ] ):

    """
    convert image to COG with gdal translate functionality
    """

    # default null return value 
    out_pathname = None
    try:

        # open existing image
        src_ds = gdal.Open( pathname, gdal.GA_ReadOnly )
        if src_ds is not None:

            # get datetime string from pathname
            dt = getDateTimeString( pathname )
            if dt is not None:

                # create out path if required
                out_path = os.path.join( args.tmp_path, dt )
                if not os.path.exists( out_path ):
                    os.makedirs( out_path )

                # execute translation - report error to log
                out_pathname = os.path.join( out_path, os.path.basename( pathname ) )
                ds = gdal.Translate(    out_pathname, 
                                        src_ds, 
                                        format='COG', 
                                        creationOptions=creationOptions )

                # error occurred
                if ds is None:
                    out_pathname = None


    except BaseException as error:
        print ( 'Error: {} '.format ( error ) )

    return out_pathname


def parseArguments( args=None ):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-cog')
    parser.add_argument( 'in_path', action='store' )
    parser.add_argument( 'out_path', action='store' )
    parser.add_argument( '-key_pathname', default=None, action='store' )
    parser.add_argument( '-chunk_size', default=5242880, action='store', type=int )
    parser.add_argument( '-tmp_path', default='C:\\Users\\crwil\\Documents\\data\\tmp', action='store' )

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()

    # assume single scene - else parse directory using input arg as glob arg
    images = [ args.in_path ]
    if not os.path.isfile( args.in_path ):
        images = glob.glob( os.path.join( args.in_path ), recursive=True )

    for image in images:

        # convert to cog
        tmp_pathname = convertToCog(  image, args )
        if tmp_pathname is not None:

            # move to cloud / local filesystem
            if GsClient.isUri( args.out_path ):
                moveToCloud( tmp_pathname, args )
            else:

                # create local output folder
                out_pathname = os.path.join( args.out_path, tmp_pathname[ len( args.tmp_path ) + 1 : ] )
                if os.path.exists( os.path.dirname( out_pathname ) ):
                    os.makedirs( os.path.dirname( out_pathname ) )

                # move cog to local out path
                shutil.move( tmp_pathname, out_pathname )

            # remove tmp directory
            shutil.rmtree( os.path.dirname ( tmp_pathname ) )
                
    return


# execute main
if __name__ == '__main__':
    main()

