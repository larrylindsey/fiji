package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.network.MessageXC;
import edu.utexas.clm.archipelago.network.translation.Bottle;

import java.awt.geom.Area;
import java.awt.geom.Path2D;
import java.awt.geom.PathIterator;
import java.io.IOException;

/**
 *
 */
public class AreaBottle implements Bottle<Area>
{
    private final double[][] path;
    private final int[] type;

    public AreaBottle(final Area area)
    {
        PathIterator pit = area.getPathIterator(null);
        int count = 0, i = 0;

        while (!pit.isDone())
        {
            ++count;
            pit.next();
        }

        pit = area.getPathIterator(null);
        path = new double[count][6];
        type = new int[count];

        while (!pit.isDone())
        {
            type[i] = pit.currentSegment(path[i]);

            pit.next();
            ++i;
        }
    }

    public Area unBottle(MessageXC xc) throws IOException
    {
        final Path2D.Double path2D = new Path2D.Double(Path2D.WIND_EVEN_ODD, type.length);

        for (int i = 0; i < type.length; ++i)
        {
            switch (type[i])
            {
                case PathIterator.SEG_CUBICTO:
                    path2D.curveTo(path[i][0], path[i][1],
                            path[i][2], path[i][3],
                            path[i][4], path[i][5]);
                    break;
                case PathIterator.SEG_LINETO:
                    path2D.lineTo(path[i][0], path[i][1]);
                    break;
                case PathIterator.SEG_MOVETO:
                    path2D.moveTo(path[i][0], path[i][1]);
                    break;
                case PathIterator.SEG_QUADTO:
                    path2D.quadTo(path[i][0], path[i][1], path[i][2], path[i][3]);
                    break;
                default:
                    // do nothing.
                    break;
            }
        }

        return new Area(path2D);
    }
}
