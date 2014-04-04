package edu.utexas.clm.archipelago.ijsupport.bottle.tile;

import edu.utexas.clm.archipelago.network.MessageXC;
import edu.utexas.clm.archipelago.network.translation.Bottle;
import ini.trakem2.display.Patch;
import mpicbg.models.Model;
import mpicbg.trakem2.align.AbstractAffineTile2D;
import mpicbg.trakem2.align.GenericAffineTile2D;

import java.io.IOException;

/**
 *
 */
public class GenericAffineTile2DBottle implements Bottle<AbstractAffineTile2D<?>>
{
    private final Model<?> model;
    private final Patch patch;

    public GenericAffineTile2DBottle(final GenericAffineTile2D tile)
    {
        model = tile.getModel();
        patch = tile.getPatch();
    }

    public AbstractAffineTile2D<?> unBottle(final MessageXC xc) throws IOException
    {
        try
        {
            return new GenericAffineTile2D(model, patch);
        }
        catch (final ClassCastException cce)
        {
            throw new IOException(cce);
        }
    }
}
